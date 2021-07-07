package oci8

import "testing"

func testUndo(t *testing.T) {
	oc := get_conn(testDSN)

	undologManager := GetUndoLogManager()

	err := undologManager.Undo(oc, testXID, testBranchID, testResourceID)

	if err != nil {
		t.Errorf("Undo Error")
	}
}
