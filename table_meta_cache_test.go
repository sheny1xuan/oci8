package oci8

import (
	"database/sql"
	"fmt"
	"log"
	"testing"
)

func TestFetchInfo(t *testing.T) {
	db, err := sql.Open("oci8", "C##STUDENT/123456@127.0.0.1:1521/ORCL")
	if err != nil {
		log.Fatal(err)
	}

	defer db.Close()

	// s := "SELECT OWNER, TABLE_NAME, COLUMN_NAME, DATA_TYPE, CHAR_LENGTH, DATA_PRECISION, " +
	// 	"DATA_SCALE, NULLABLE, DATA_DEFAULT, DATA_LENGTH, COLUMN_ID FROM ALL_TAB_COLUMNS WHERE TABLE_NAME = "
	// tablename := "AAASWPAAHAAAAIEAAC"
	// s := "SELECT OWNER, TABLE_NAME FROM ALL_TAB_COLUMNS WHERE TABLE_NAME = :1 "
	// s := "SELECT CON.INDEX_NAME, COL.COLUMN_NAME, IDX.UNIQUENESS " +
	// 	"FROM USER_CONSTRAINTS CON,  USER_CONS_COLUMNS COL, ALL_INDEXES IDX " +
	// 	"WHERE CON.CONSTRAINT_NAME = COL.CONSTRAINT_NAME " +
	// 	"AND idx.INDEX_NAME = con.INDEX_NAME " +
	// 	"AND CON.CONSTRAINT_TYPE='P' AND COL.TABLE_NAME = "
	// s = fmt.Sprintf(s, tablename)
	// s := "SELECT OWNER, TABLE_NAME FROM ALL_TAB_COLUMNS WHERE TABLE_NAME = "
	// s := "SELECT COL1,COL2 FROM test  WHERE \"ROWID\" IN (:1)"

	rows, err := db.Query(SelectUndoLogSql, testXID, testBranchID)
	// fmt.Println(s)

	if err != nil {
		t.Errorf("Query Error")
	}

	for rows.Next() {
		var (
			retXID          string
			retBranchID     int64
			retContext      string
			retRollbackInfo []byte
			retState        int64
		)
		// var f3 string
		rows.Scan(&retBranchID, &retXID, &retContext, &retRollbackInfo, &retState)
		// rows.Scan(&f1)
		fmt.Printf("XID:%s, branchID:%d, Context:%s, State:%d \n", retXID, retBranchID, retContext, retState)
		fmt.Printf("RollbackInfo: %x\n", retRollbackInfo)
	}

	rows.Close()

}

func TestInsertUndologManager(t *testing.T) {

	db, err := sql.Open("oci8", "C##STUDENT/123456@127.0.0.1:1521/ORCL")
	// testundoLogID := int64(1234)
	testBranchID := int64(12345)
	testXID := "123.123:123456"
	testctx := "dfjjajaf"
	testRollbackInfo := []byte("adbdfbadf")
	testLogStatus := int64(1)

	if err != nil {
		log.Fatal(err)
	}

	_, err = db.Exec(InsertUndoLogSql, testBranchID, testXID, testctx, testRollbackInfo, testLogStatus)

	if err != nil {
		log.Fatal(err)
	}
}
