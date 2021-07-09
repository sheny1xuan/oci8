package oci8

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"fmt"
	"log"
	"testing"
)

var (
	col1Val string  = "a"
	col2Val int64   = 123
	col3Val float64 = 12.33
	col4Val         = []byte{1, 2, 3}
)

func TestUndo(t *testing.T) {
	execUndo(t)
}

func TestInsertUndoManager(t *testing.T) {

	getInsertUndolog(t)

	execUndo(t)
}

func TestUpdateUndoManager(t *testing.T) {

	getUpdateUndolog(t)

	execUndo(t)
}

func TestDeleteUndoManager(t *testing.T) {

	getInsertUndolog(t)

	execUndo(t)
}

func getInsertUndolog(t *testing.T) {

	InitTableMetaCache("C##STUDENT")

	tx := get_tx(testDSN)

	args := []driver.Value{col1Val, col2Val, col3Val, col4Val}

	stmt, err := tx.conn.Prepare("insert into test (col1, col2, col3, col4, col5 ) values ( :1, :2, :3, :4, sysdate)")

	if err != nil {
		t.Errorf("Insert error")
	}

	s := stmt.(*Stmt)

	_, err = s.Exec(args)

	if err != nil {
		t.Errorf("Insert error")
	}

	tx.conn.ctx.branchID = testBranchID

	if len(tx.conn.ctx.sqlUndoItemsBuffer) > 0 {
		err = GetUndoLogManager().FlushUndoLogs(tx.conn)
	}

	tx.localCommit()
}

func getDeleteUndolog(t *testing.T) {
	db, err := sql.Open("oci8", "C##STUDENT/123456@127.0.0.1:1521/ORCL")
	if err != nil {
		log.Fatal(err)
	}

	defer db.Close()

	InitTableMetaCache("C##STUDENT")

	ctx := context.WithValue(
		context.Background(),
		XID,
		testXID)

	err = insertDataN(3, db)

	if err != nil {
		t.Errorf("Insert error")
	}

	tx, err := db.BeginTx(ctx, &sql.TxOptions{
		Isolation: sql.LevelDefault,
		ReadOnly:  false,
	})

	if err != nil {
		t.Errorf("begin Tx error")
	}

	// Delete
	tx.Exec("DELETE FROM TEST WHERE COL1 = :1 AND col2 = :2", col1Val, col2Val)

	tx.Commit()
}

func getUpdateUndolog(t *testing.T) {
	db, err := sql.Open("oci8", "C##STUDENT/123456@127.0.0.1:1521/ORCL")
	if err != nil {
		log.Fatal(err)
	}

	defer db.Close()

	InitTableMetaCache("C##STUDENT")

	ctx := context.WithValue(
		context.Background(),
		XID,
		testXID)

	err = insertDataN(3, db)

	if err != nil {
		t.Errorf("Insert error")
	}

	tx, err := db.BeginTx(ctx, &sql.TxOptions{
		Isolation: sql.LevelDefault,
		ReadOnly:  false,
	})

	if err != nil {
		t.Errorf("begin Tx error")
	}

	// Update
	tx.Exec("UPDATE TEST SET COL1 = :1, COL2 = :2, COL3 = :3 WHERE COL1 = :4", "X", 88, 88.88, col1Val)

	tx.Commit()
}

// execute undolog
func execUndo(t *testing.T) {
	oc := get_conn(testDSN)

	InitTableMetaCache(oc.cfg.Username)

	undologManager := GetUndoLogManager()

	err := undologManager.Undo(oc, testXID, testBranchID, testResourceID)

	if err != nil {
		t.Errorf("Undo Error")
	}
}

func TestSelectUndoLog(t *testing.T) {
	oc := get_conn(testDSN)
	args := []driver.Value{testXID, testBranchID}
	rows, err := oc.prepareQuery(SelectUndoLogSql, args)

	if err != nil {
		t.Errorf("Undo Qury Error")
	}

	vals := make([]driver.Value, 5)

	var (
		retXID          string
		retBranchID     int64
		retContext      string
		retRollbackInfo []byte
		retState        int64
	)

	for {
		err := rows.Next(vals)

		if err != nil {
			// t.Errorf("Get info error")
			break
		}

		retXID = vals[1].(string)
		retBranchID = vals[0].(int64)
		retContext = vals[2].(string)
		retRollbackInfo = vals[3].([]byte)
		retState = vals[4].(int64)

		fmt.Printf("XID:%s, branchID:%d, Context:%s, State:%d \n", retXID, retBranchID, retContext, retState)
		fmt.Printf("RollbackInfo: %x\n", retRollbackInfo)

	}

}
