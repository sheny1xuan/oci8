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
	return stmt, nil
}
