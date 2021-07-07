package oci8

import (
	"database/sql/driver"
	"fmt"
	"testing"
)

func TestUndo(t *testing.T) {
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
