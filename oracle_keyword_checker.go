package oci8

import "strings"

// 110 key words
var ORACLEKeyword = map[string]string{
	"ACCESS":          "ACCESS",
	"ADD":             "ADD",
	"ALL":             "ALL",
	"ALTER":           "ALTER",
	"AND":             "AND",
	"ANY":             "ANY",
	"AS":              "AS",
	"ASC":             "ASC",
	"AUDIT":           "AUDIT",
	"BETWEEN":         "BETWEEN",
	"BY":              "BY",
	"CHAR":            "CHAR",
	"CHECK":           "CHECK",
	"CLUSTER":         "CLUSTER",
	"COLUMN":          "COLUMN",
	"COLUMN_VALUE":    "COLUMN_VALUE",
	"COMMENT":         "COMMENT",
	"COMPRESS":        "COMPRESS",
	"CONNECT":         "CONNECT",
	"CREATE":          "CREATE",
	"CURRENT":         "CURRENT",
	"DATE":            "DATE",
	"DECIMAL":         "DECIMAL",
	"DEFAULT":         "DEFAULT",
	"DELETE":          "DELETE",
	"DESC":            "DESC",
	"DISTINCT":        "DISTINCT",
	"DROP":            "DROP",
	"ELSE":            "ELSE",
	"EXCLUSIVE":       "EXCLUSIVE",
	"EXISTS":          "EXISTS",
	"FILE":            "FILE",
	"FLOAT":           "FLOAT",
	"FOR":             "FOR",
	"FROM":            "FROM",
	"GRANT":           "GRANT",
	"GROUP":           "GROUP",
	"HAVING":          "HAVING",
	"IDENTIFIED":      "IDENTIFIED",
	"IMMEDIATE":       "IMMEDIATE",
	"IN":              "IN",
	"INCREMENT":       "INCREMENT",
	"INDEX":           "INDEX",
	"INITIAL":         "INITIAL",
	"INSERT":          "INSERT",
	"INTEGER":         "INTEGER",
	"INTERSECT":       "INTERSECT",
	"INTO":            "INTO",
	"IS":              "IS",
	"LEVEL":           "LEVEL",
	"LIKE":            "LIKE",
	"LOCK":            "LOCK",
	"LONG":            "LONG",
	"MAXEXTENTS":      "MAXEXTENTS",
	"MINUS":           "MINUS",
	"MLSLABEL":        "MLSLABEL",
	"MODE":            "MODE",
	"MODIFY":          "MODIFY",
	"NESTED_TABLE_ID": "NESTED_TABLE_ID",
	"NOAUDIT":         "NOAUDIT",
	"NOCOMPRESS":      "NOCOMPRESS",
	"NOT":             "NOT",
	"NOWAIT":          "NOWAIT",
	"NULL":            "NULL",
	"NUMBER":          "NUMBER",
	"OF":              "OF",
	"OFFLINE":         "OFFLINE",
	"ON":              "ON",
	"ONLINE":          "ONLINE",
	"OPTION":          "OPTION",
	"OR":              "OR",
	"ORDER":           "ORDER",
	"PCTFREE":         "PCTFREE",
	"PRIOR":           "PRIOR",
	"PUBLIC":          "PUBLIC",
	"RAW":             "RAW",
	"RENAME":          "RENAME",
	"RESOURCE":        "RESOURCE",
	"REVOKE":          "REVOKE",
	"ROW":             "ROW",
	"ROWID":           "ROWID",
	"ROWNUM":          "ROWNUM",
	"ROWS":            "ROWS",
	"SELECT":          "SELECT",
	"SESSION":         "SESSION",
	"SET":             "SET",
	"SHARE":           "SHARE",
	"SIZE":            "SIZE",
	"SMALLINT":        "SMALLINT",
	"START":           "START",
	"SUCCESSFUL":      "SUCCESSFUL",
	"SYNONYM":         "SYNONYM",
	"SYSDATE":         "SYSDATE",
	"TABLE":           "TABLE",
	"THEN":            "THEN",
	"TO":              "TO",
	"TRIGGER":         "TRIGGER",
	"UID":             "UID",
	"UNION":           "UNION",
	"UNIQUE":          "UNIQUE",
	"UPDATE":          "UPDATE",
	"USER":            "USER",
	"VALIDATE":        "VALIDATE",
	"VALUES":          "VALUES",
	"VARCHAR":         "VARCHAR",
	"VARCHAR2":        "VARCHAR2",
	"VIEW":            "VIEW",
	"WHENEVER":        "WHENEVER",
	"WHERE":           "WHERE",
	"WITH":            "WITH",
}

// Check oralce 保留字
func Check(fieldOrTableName string) bool {
	_, ok := ORACLEKeyword[fieldOrTableName]
	if ok {
		return true
	}
	if fieldOrTableName != "" {
		fieldOrTableName = strings.ToUpper(fieldOrTableName)
	}
	_, ok = ORACLEKeyword[fieldOrTableName]
	return ok
}

func CheckEscape(fieldOrTableName string) bool {
	return Check(fieldOrTableName)
}

func CheckAndReplace(fieldOrTableName string) string {
	if Check(fieldOrTableName) {
		return "\"" + fieldOrTableName + "\""
	} else {
		return fieldOrTableName
	}
}
