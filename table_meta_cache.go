package oci8

import (
	"database/sql/driver"
	"fmt"
	"strings"
	"time"

	"github.com/sheny1xuan/oci8/schema"

	"github.com/google/go-cmp/cmp"
	// "github.com/opentrx/mysql/schema"
	"github.com/patrickmn/go-cache"
	"github.com/pkg/errors"
)

var EXPIRE_TIME = 15 * time.Minute

var tableMetaCaches map[string]*TableMetaCache = make(map[string]*TableMetaCache, 0)

type TableMetaCache struct {
	tableMetaCache *cache.Cache
	dbName         string
}

func InitTableMetaCache(dbName string) {
	tableMetaCache := &TableMetaCache{
		tableMetaCache: cache.New(EXPIRE_TIME, 10*EXPIRE_TIME),
		dbName:         dbName,
	}
	tableMetaCaches[dbName] = tableMetaCache
}

func GetTableMetaCache(dbName string) (*TableMetaCache, error) {
	tableMetaName, ok := tableMetaCaches[dbName]
	if ok {
		return tableMetaName, nil
	} else {
		return tableMetaName, errors.Errorf("%s not in tableMetaCache", dbName)
	}
}

func (cache *TableMetaCache) GetTableMeta(conn *Conn, tableName string) (schema.TableMeta, error) {
	if tableName == "" {
		return schema.TableMeta{}, errors.New("TableMeta cannot be fetched without tableName")
	}
	cacheKey := cache.GetCacheKey(tableName)
	tMeta, found := cache.tableMetaCache.Get(cacheKey)
	// meta is column infos and index infos
	if found {
		meta := tMeta.(schema.TableMeta)
		return meta, nil
	} else {
		meta, err := cache.FetchSchema(conn, tableName)
		if err != nil {
			return schema.TableMeta{}, errors.WithStack(err)
		}
		cache.tableMetaCache.Set(cacheKey, meta, EXPIRE_TIME)
		return meta, nil
	}
}

func (cache *TableMetaCache) Refresh(conn *Conn, resourceID string) {
	for k, v := range cache.tableMetaCache.Items() {
		meta := v.Object.(schema.TableMeta)
		key := cache.GetCacheKey(meta.TableName)
		if k == key {
			tMeta, err := cache.FetchSchema(conn, meta.TableName)
			if err != nil {
				// fmt.Println("get table meta error:%s", err.Error())
				fmt.Println("error")
			}
			if !cmp.Equal(tMeta, meta) {
				cache.tableMetaCache.Set(key, tMeta, EXPIRE_TIME)
			}
		}
	}
}

func (cache *TableMetaCache) GetCacheKey(tableName string) string {
	return fmt.Sprintf("%s.%s", cache.dbName, escape2(tableName, "`"))
}

func (cache *TableMetaCache) FetchSchema(conn *Conn, tableName string) (schema.TableMeta, error) {
	tm := schema.TableMeta{TableName: tableName,
		AllColumns: make(map[string]schema.ColumnMeta),
		AllIndexes: make(map[string]schema.IndexMeta),
	}
	columnMetas, err := GetColumns(conn, cache.dbName, tableName)
	if err != nil {
		return schema.TableMeta{}, errors.Wrapf(err, "Could not found any index in the table: %s", tableName)
	}
	columns := make([]string, 0)
	for _, column := range columnMetas {
		tm.AllColumns[column.ColumnName] = column
		columns = append(columns, column.ColumnName)
	}
	tm.Columns = columns
	indexes, err := GetIndexes(conn, cache.dbName, tableName)
	if err != nil {
		return schema.TableMeta{}, errors.Wrapf(err, "Could not found any index in the table: %s", tableName)
	}
	// 获取每个索引主键对应的列的信息
	for _, index := range indexes {
		col := tm.AllColumns[index.ColumnName]
		idx, ok := tm.AllIndexes[index.IndexName]
		if ok {
			idx.Values = append(idx.Values, col)
		} else {
			index.Values = append(index.Values, col)
			tm.AllIndexes[index.IndexName] = index
		}
	}
	if len(tm.AllIndexes) == 0 {
		return schema.TableMeta{}, errors.Errorf("Could not found any index in the table: %s", tableName)
	}

	return tm, nil
}

func GetColumns(conn *Conn, dbName, tableName string) ([]schema.ColumnMeta, error) {
	var tn = escape2(tableName, "`")
	tn = strings.ToUpper(tn)
	tn = fmt.Sprintf("'%s'", tn)
	args := []driver.Value{}
	//`TABLE_CATALOG`,	`TABLE_SCHEMA`,	`TABLE_NAME`,	`COLUMN_NAME`,	`ORDINAL_POSITION`,	`COLUMN_DEFAULT`,
	//`IS_NULLABLE`, `DATA_TYPE`,	`CHARACTER_MAXIMUM_LENGTH`,	`CHARACTER_OCTET_LENGTH`,	`NUMERIC_PRECISION`,
	//`NUMERIC_SCALE`, `DATETIME_PRECISION`, `CHARACTER_SET_NAME`,	`COLLATION_NAME`,	`COLUMN_TYPE`,	`COLUMN_KEY',
	//`EXTRA`,	`PRIVILEGES`, `COLUMN_COMMENT`, `GENERATION_EXPRESSION`, `SRS_ID`

	// TABLE_CATALOG ->
	// TABLE_SCHEMA -> OWNER
	// TABLE_NAME -> TABLE_NAME
	// COLUMN_NAME -> COLUMN_NAME
	// DATA_TYPE -> DATA_TYPE
	// CHARACTER_MAXIMUM_LENGTH -> CHAR_LENGTH

	// NUMERIC_PRECISION -> DATA_PRECISION
	// NUMERIC_SCALE -> DATA_SCALE
	// IS_NULLABLE -> NULLABLE
	// COLUMN_COMMENT
	// COLUMN_DEFAULT -> DATA_DEFAULT
	// CHARACTER_OCTET_LENGTH -> DATA_LENGTH ? 不确定

	// ORDINAL_POSITION	-> COLUMN_ID字段标识
	// COLUMN_KEY	索引类型
	// EXTRA	是否递增

	// INFORMATION_SCHEMA`.`COLUMNS ->
	// s := "SELECT `TABLE_CATALOG`, `TABLE_SCHEMA`, `TABLE_NAME`, `COLUMN_NAME`, `DATA_TYPE`, `CHARACTER_MAXIMUM_LENGTH`, " +
	// 	"`NUMERIC_PRECISION`, `NUMERIC_SCALE`, `IS_NULLABLE`, `COLUMN_COMMENT`, `COLUMN_DEFAULT`, `CHARACTER_OCTET_LENGTH`, " +
	// 	"`ORDINAL_POSITION`, `COLUMN_KEY`, `EXTRA`  FROM `INFORMATION_SCHEMA`.`COLUMNS` WHERE `TABLE_SCHEMA` = ? AND `TABLE_NAME` = ?"

	// undo -> oracle datatype different mysql
	s := "SELECT OWNER, TABLE_NAME, COLUMN_NAME, DATA_TYPE, CHAR_LENGTH, DATA_PRECISION, " +
		"DATA_SCALE, NULLABLE, DATA_DEFAULT, DATA_LENGTH, COLUMN_ID FROM ALL_TAB_COLUMNS WHERE TABLE_NAME = "
	rows, err := conn.prepareQuery(s+tn, args)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	result := make([]schema.ColumnMeta, 0)

	var tableCat, tScheme, tName, columnName, dataType, isNullable, remark, colDefault, extra string
	var columnSize, decimalDigits, numPreRadix, charOctetLength, ordinalPosition float64

	vals := make([]driver.Value, 11)
	dest := []interface{}{
		&tScheme, &tName, &columnName, &dataType,
		&columnSize, &decimalDigits, &numPreRadix, &isNullable,
		&colDefault, &charOctetLength, &ordinalPosition,
	}

	// dest := []interface{}{
	// 	&tableCat, &tScheme, &tName, &columnName, &dataType,
	// 	&columnSize, &decimalDigits, &numPreRadix, &isNullable,
	// 	&remark, &colDefault, &charOctetLength, &ordinalPosition,
	// 	&colKey, &extra,
	// }

	for {
		err := rows.Next(vals)
		if err != nil {
			break
		}

		for i, sv := range vals {
			err := convertAssignRows(dest[i], sv)
			if err != nil {
				return nil, fmt.Errorf(`sql: Scan error on column index %d, name %q: %v`, i, rows.Columns()[i], err)
			}
		}

		col := schema.ColumnMeta{}

		col.TableCat = tableCat
		col.TableSchemeName = tScheme
		col.TableName = tName
		col.ColumnName = columnName
		col.DataTypeName = dataType
		col.DataType = GetSqlDataType(dataType)
		col.ColumnSize = int32(columnSize)
		col.DecimalDigits = int32(decimalDigits)
		col.NumPrecRadix = int32(numPreRadix)
		col.IsNullable = isNullable
		if strings.ToUpper(isNullable) == "N" {
			col.Nullable = 0
		} else {
			col.Nullable = 1
		}
		col.Remarks = remark
		col.ColumnDef = colDefault
		col.SqlDataType = 0
		col.SqlDatetimeSub = 0
		col.CharOctetLength = int32(charOctetLength)
		col.OrdinalPosition = int32(ordinalPosition)
		col.IsAutoIncrement = extra

		// col := schema.ColumnMeta{}

		// col.TableCat = ""
		// col.TableSchemeName = vals[0].(string)
		// col.TableName = vals[1].(string)
		// col.ColumnName = vals[2].(string)
		// col.DataTypeName = vals[3].(string)
		// col.DataType = GetSqlDataType(col.DataTypeName)
		// col.ColumnSize = int32(vals[4].(float64))
		// col.DecimalDigits = int32(vals[5].(float64))
		// col.NumPrecRadix = int32(vals[6].(float64))
		// col.IsNullable = vals[7].(string)
		// if strings.ToLower(col.IsNullable) == "N" {
		// 	col.Nullable = 0
		// } else {
		// 	col.Nullable = 1
		// }
		// // col.Remarks = remark.String
		// col.ColumnDef = vals[8].(string)
		// col.SqlDataType = 0
		// col.SqlDatetimeSub = 0
		// col.CharOctetLength = int32(vals[9].(float64))
		// col.OrdinalPosition = int32(vals[10].(float64))
		// // col.IsAutoIncrement = extra.String

		result = append(result, col)
	}
	return result, nil
}

func GetIndexes(conn *Conn, dbName, tableName string) ([]schema.IndexMeta, error) {
	var tn = escape2(tableName, "`")
	tn = strings.ToUpper(tn)
	tn = fmt.Sprintf("'%s'", tn)
	args := []driver.Value{}
	// undo 只拿出主键，应该拿出来所有索引
	s := "SELECT CON.INDEX_NAME, COL.COLUMN_NAME, IDX.UNIQUENESS, CON.CONSTRAINT_TYPE " +
		"FROM USER_CONSTRAINTS CON,  USER_CONS_COLUMNS COL, ALL_INDEXES IDX " +
		"WHERE CON.CONSTRAINT_NAME = COL.CONSTRAINT_NAME " +
		"AND idx.INDEX_NAME = con.INDEX_NAME " +
		"AND CON.CONSTRAINT_TYPE='P' AND COL.TABLE_NAME = "

	rows, err := conn.prepareQuery(s+tn, args)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	result := make([]schema.IndexMeta, 0)

	var indexName, columnName, nonUnique, indexType, collation string
	var ordinalPosition, cardinality float64

	vals := make([]driver.Value, 4)
	dest := []interface{}{
		&indexName, &columnName, &nonUnique, &indexType,
	}

	for {
		err := rows.Next(vals)
		if err != nil {
			break
		}

		for i, sv := range vals {
			err := convertAssignRows(dest[i], sv)
			if err != nil {
				return nil, fmt.Errorf(`sql: Scan error on column index %d, name %q: %v`, i, rows.Columns()[i], err)
			}
		}

		index := schema.IndexMeta{
			Values: make([]schema.ColumnMeta, 0),
		}

		index.IndexName = indexName
		index.ColumnName = columnName
		if "UNIQUE" == strings.ToUpper(nonUnique) {
			index.NonUnique = true
		}
		index.OrdinalPosition = int32(ordinalPosition)
		index.AscOrDesc = collation
		index.Cardinality = int32(cardinality)
		if "P" == strings.ToUpper(indexType) {
			index.IndexType = schema.IndexType_PRIMARY
		} else if !index.NonUnique {
			index.IndexType = schema.IndexType_UNIQUE
		} else {
			index.IndexType = schema.IndexType_NORMAL
		}

		result = append(result, index)
	}
	return result, nil
}

func escape2(tableName, cutset string) string {
	var tn = tableName
	if strings.Contains(tableName, ".") {
		idx := strings.LastIndex(tableName, ".")
		tName := tableName[idx+1:]
		tn = strings.Trim(tName, cutset)
	} else {
		tn = strings.Trim(tableName, cutset)
	}
	return tn
}
