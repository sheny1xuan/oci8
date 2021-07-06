package oci8

import (
	"context"
	"database/sql"
	"io/ioutil"
	"log"
	"testing"
)

var test_DSN string = "C##STUDENT/123456@127.0.0.1:1521/ORCL"

func get_conn(dbname string) *Conn {

	db := DriverStruct{Logger: log.New(ioutil.Discard, "", 0)}
	conn, err := db.Open(test_DSN)
	if err != nil {
		panic("connect erroe")
	}

	oc := conn.(*Conn)

	return oc
}

func TestConn(t *testing.T) {
	oc := get_conn(test_DSN)
	defer oc.Close()
}

func TestExctor(t *testing.T) {
	db, err := sql.Open("oci8", "C##STUDENT/123456@127.0.0.1:1521/ORCL")
	if err != nil {
		log.Fatal(err)
	}

	defer db.Close()
	InitTableMetaCache("C##STUDENT")
	ctx := context.WithValue(
		context.Background(),
		XID,
		"192.2:1231:120301")

	tx, err := db.BeginTx(ctx, &sql.TxOptions{
		Isolation: sql.LevelDefault,
		ReadOnly:  false,
	})

	if err != nil {
		t.Errorf("begin Tx error")
	}
	col1Val := "a"
	col2Val := 123
	col3Val := 12.33
	col4Val := []byte{1, 2, 3}
	// "insert into test ( col1, col2 ) values ( :1, :2)", "日文", "BB"

	// Insert
	// tx.Exec("insert into test ( col1, col2, col3, col4 ) values ( :1, :2, :3, :4)", col1Val, col2Val, col3Val, col4Val)

	// Delete
	tx.Exec("DELETE FROM TEST WHERE (COL1, COL2) in (:1, :2)", col1Val, col2Val)

	tx.Commit()

}
