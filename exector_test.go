package oci8

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"fmt"
	"io/ioutil"
	"log"
	"testing"
)

var (
	testDSN        string = "C##STUDENT/123456@127.0.0.1:1521/ORCL"
	testXID        string = "192.2:1231:120301"
	testBranchID   int64  = 1231
	testResourceID string = "41"
)

func get_conn(dbname string) *Conn {

	db := DriverStruct{Logger: log.New(ioutil.Discard, "", 0)}
	conn, err := db.Open(testDSN)
	if err != nil {
		panic("connect erroe")
	}

	oc := conn.(*Conn)

	return oc
}

func get_tx(dbname string) *Tx {

	conn := get_conn(dbname)

	ctx := context.WithValue(
		context.Background(),
		XID,
		testXID)

	tx, err := conn.BeginTx(ctx, driver.TxOptions{
		ReadOnly: false,
	})

	if err != nil {
		panic("Get Tx error")
	}

	ret, _ := tx.(*Tx)

	return ret
}

func TestConn(t *testing.T) {
	oc := get_conn(testDSN)
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
		testXID)

	tx, err := db.BeginTx(ctx, &sql.TxOptions{
		Isolation: sql.LevelDefault,
		ReadOnly:  false,
	})

	if err != nil {
		t.Errorf("begin Tx error")
	}
	col1Val := "a"
	// col2Val := 123
	// col3Val := 12.33
	// col4Val := []byte{1, 2, 3}

	// Insert
	// tx.Exec("insert into test (col1, col2, col3, col4 ) values ( :1, :2, :3, :4)", col1Val, col2Val, col3Val, col4Val)

	// Delete
	// tx.Exec("DELETE FROM TEST WHERE COL1 = :1 AND col2 = :2", col1Val, col2Val)

	// Update
	tx.Exec("UPDATE TEST SET COL1 = :1, COL2 = :2, COL3 = :3 WHERE COL1 = :4", "X", 88, 88.88, col1Val)

	// Normal Qury
	// rows, err := tx.Query("SELECT COL1, COL2, COL3, COL4 FROM TEST WHERE COL1 = :1 AND COL2 = :2", "X", 88)

	// Update Qury
	// rows, err := tx.Query("SELECT COL1, COL2, COL3, COL4 FROM TEST WHERE COL1 = :1 AND COL2 = :2 FOR UPDATE", "X", 88)
	// for rows.Next() {
	// 	rows.Scan(&col1Val, &col2Val, &col3Val, &col4Val)

	// 	fmt.Println(col1Val)
	// 	fmt.Println(col2Val)
	// 	fmt.Println(col3Val)
	// 	for _, b := range col4Val {
	// 		fmt.Printf("%x", b)
	// 	}
	// 	fmt.Printf("/n")
	// }

	tx.Commit()

}

func TestBasicUse(t *testing.T) {
	db, err := sql.Open("oci8", "C##STUDENT/123456@127.0.0.1:1521/ORCL")
	if err != nil {
		t.Errorf("Open sql Error")
	}

	col1Val := "a"
	col2Val := 123
	col3Val := 12.33
	col4Val := []byte{1, 2, 3}

	// INSERT
	for i := 0; i < 3; i++ {
		if i != 0 {
			_, err = db.Exec("insert into test ( col1, col2, col3, col4 ) values ( :1, :2, :3, :4)", col1Val+fmt.Sprint(i), col2Val+i, col3Val+float64(i), col4Val)
		} else {
			_, err = db.Exec("insert into test ( col1, col2, col3, col4 ) values ( :1, :2, :3, :4)", col1Val, col2Val+i, col3Val+float64(i), col4Val)
		}

		if err != nil {
			t.Errorf("INSERT error")
		}
	}

	// DELETE
	_, err = db.Exec("DELETE FROM TEST WHERE \"COL1\" = :1 AND \"COL2\" = :2", col1Val+fmt.Sprint(1), col2Val+1)

	if err != nil {
		t.Errorf("DELETE error")
	}

	// UPDATE

	_, err = db.Exec("UPDATE TEST SET COL1 = :1 ,col2 = :2 WHERE col1 = :3", col1Val, col2Val, col1Val+fmt.Sprint(2))

	if err != nil {
		t.Errorf("UPDATE error")
	}

	db.Close()

}

func insertDataN(x int, db *sql.DB) error {
	for i := 0; i < x; i++ {
		_, err := db.Exec("insert into test ( col1, col2, col3, col4 ) values ( :1, :2, :3, :4)", col1Val, col2Val, col3Val, col4Val)

		if err != nil {
			return err
		}
	}
	return nil
}
