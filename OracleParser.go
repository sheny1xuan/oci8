package oci8

import (
	"fmt"
	"strings"
)

func oracle2Mysql(stmt string) (string, error) {

	signCnt := strings.Count(stmt, ":")
	if signCnt != 0 {
		var ok bool = true
		for i := 0; i < signCnt; i++ {
			t_string := ":" + fmt.Sprint(i+1)
			idx := strings.Index(stmt, t_string)

			if idx == -1 {
				ok = false
				break
			}

			stmt = strings.Replace(stmt, t_string, "?", 1)
		}
		if !ok {
			err := fmt.Errorf("Parse oracle Error")
			return stmt, err
		} else {
			return stmt, nil
		}
	} else {
		return stmt, nil
	}
}

func mysql2OracleWhere(stmt string) (string, error) {

	signCnt := strings.Count(stmt, "?")
	if signCnt != 0 {
		var ok bool = true
		for i := 0; i < signCnt; i++ {
			t_string := ":" + fmt.Sprint(i+1)
			idx := strings.Index(stmt, "?")

			if idx == -1 {
				ok = false
				break
			}

			stmt = strings.Replace(stmt, "?", t_string, 1)
		}
		if !ok {
			err := fmt.Errorf("Parse oracle Error")
			return stmt, err
		}
	}
	stmt = strings.ReplaceAll(stmt, "`", "\"")
	stmt = strings.ToUpper(stmt)
	return stmt, nil
}

func AppendInParam(size int) string {
	var sb strings.Builder
	fmt.Fprintf(&sb, "(")
	for i := 0; i < size; i++ {
		fmt.Fprintf(&sb, ":"+fmt.Sprint(i+1))
		if i < size-1 {
			fmt.Fprint(&sb, ",")
		}
	}
	fmt.Fprintf(&sb, ")")
	return sb.String()
}
