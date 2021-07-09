package oci8

import (
	"database/sql/driver"
	"errors"
	"fmt"
	"reflect"
	"strconv"
	"time"
)

var errNilPtr = errors.New("destination pointer is nil")

type RawBytes []byte

func convertAssignRows(dest, src interface{}) error {
	// Common cases, without reflect.
	switch s := src.(type) {
	case string:
		switch d := dest.(type) {
		case *string:
			if d == nil {
				return errNilPtr
			}
			*d = s
			return nil
		case *[]byte:
			if d == nil {
				return errNilPtr
			}
			*d = []byte(s)
			return nil
		case *RawBytes:
			if d == nil {
				return errNilPtr
			}
			*d = append((*d)[:0], s...)
			return nil
		}
	case []byte:
		switch d := dest.(type) {
		case *string:
			if d == nil {
				return errNilPtr
			}
			*d = string(s)
			return nil
		case *interface{}:
			if d == nil {
				return errNilPtr
			}
			*d = cloneBytes(s)
			return nil
		case *[]byte:
			if d == nil {
				return errNilPtr
			}
			*d = cloneBytes(s)
			return nil
		case *RawBytes:
			if d == nil {
				return errNilPtr
			}
			*d = s
			return nil
		}
	case time.Time:
		switch d := dest.(type) {
		case *time.Time:
			*d = s
			return nil
		case *string:
			*d = s.Format(time.RFC3339Nano)
			return nil
		case *[]byte:
			if d == nil {
				return errNilPtr
			}
			*d = []byte(s.Format(time.RFC3339Nano))
			return nil
		case *RawBytes:
			if d == nil {
				return errNilPtr
			}
			*d = s.AppendFormat((*d)[:0], time.RFC3339Nano)
			return nil
		}
	// case decimalDecompose:
	// 	switch d := dest.(type) {
	// 	case decimalCompose:
	// 		return d.Compose(s.Decompose(nil))
	// 	}
	case nil:
		switch d := dest.(type) {
		case *interface{}:
			if d == nil {
				return errNilPtr
			}
			*d = nil
			return nil
		case *[]byte:
			if d == nil {
				return errNilPtr
			}
			*d = nil
			return nil
		case *RawBytes:
			if d == nil {
				return errNilPtr
			}
			*d = nil
			return nil
		case *float64:
			if d == nil {
				return errNilPtr
			}
			*d = 0
			return nil
		case *string:
			if d == nil {
				return errNilPtr
			}
			*d = ""
			return nil
		}

	}

	// if scanner, ok := dest.(Scanner); ok {
	// 	return scanner.Scan(src)
	// }

	var sv reflect.Value

	switch d := dest.(type) {
	case *string:
		sv = reflect.ValueOf(src)
		switch sv.Kind() {
		case reflect.Bool,
			reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64,
			reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64,
			reflect.Float32, reflect.Float64:
			*d = asString(src)
			return nil
		}
	case *[]byte:
		sv = reflect.ValueOf(src)
		if b, ok := asBytes(nil, sv); ok {
			*d = b
			return nil
		}
	case *RawBytes:
		sv = reflect.ValueOf(src)
		if b, ok := asBytes([]byte(*d)[:0], sv); ok {
			*d = RawBytes(b)
			return nil
		}
	case *bool:
		bv, err := driver.Bool.ConvertValue(src)
		if err == nil {
			*d = bv.(bool)
		}
		return err
	case *interface{}:
		*d = src
		return nil
	}

	dpv := reflect.ValueOf(dest)
	if dpv.Kind() != reflect.Ptr {
		return errors.New("destination not a pointer")
	}
	if dpv.IsNil() {
		return errNilPtr
	}

	if !sv.IsValid() {
		sv = reflect.ValueOf(src)
	}

	dv := reflect.Indirect(dpv)
	if sv.IsValid() && sv.Type().AssignableTo(dv.Type()) {
		switch b := src.(type) {
		case []byte:
			dv.Set(reflect.ValueOf(cloneBytes(b)))
		default:
			dv.Set(sv)
		}
		return nil
	}

	if dv.Kind() == sv.Kind() && sv.Type().ConvertibleTo(dv.Type()) {
		dv.Set(sv.Convert(dv.Type()))
		return nil
	}

	// The following conversions use a string value as an intermediate representation
	// to convert between various numeric types.
	//
	// This also allows scanning into user defined types such as "type Int int64".
	// For symmetry, also check for string destination types.
	switch dv.Kind() {
	case reflect.Ptr:
		if src == nil {
			dv.Set(reflect.Zero(dv.Type()))
			return nil
		}
		dv.Set(reflect.New(dv.Type().Elem()))
		return convertAssignRows(dv.Interface(), src)
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		if src == nil {
			return fmt.Errorf("converting NULL to %s is unsupported", dv.Kind())
		}
		s := asString(src)
		i64, err := strconv.ParseInt(s, 10, dv.Type().Bits())
		if err != nil {
			err = strconvErr(err)
			return fmt.Errorf("converting driver.Value type %T (%q) to a %s: %v", src, s, dv.Kind(), err)
		}
		dv.SetInt(i64)
		return nil
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		if src == nil {
			return fmt.Errorf("converting NULL to %s is unsupported", dv.Kind())
		}
		s := asString(src)
		u64, err := strconv.ParseUint(s, 10, dv.Type().Bits())
		if err != nil {
			err = strconvErr(err)
			return fmt.Errorf("converting driver.Value type %T (%q) to a %s: %v", src, s, dv.Kind(), err)
		}
		dv.SetUint(u64)
		return nil
	case reflect.Float32, reflect.Float64:
		if src == nil {
			return fmt.Errorf("converting NULL to %s is unsupported", dv.Kind())
		}
		s := asString(src)
		f64, err := strconv.ParseFloat(s, dv.Type().Bits())
		if err != nil {
			err = strconvErr(err)
			return fmt.Errorf("converting driver.Value type %T (%q) to a %s: %v", src, s, dv.Kind(), err)
		}
		dv.SetFloat(f64)
		return nil
	case reflect.String:
		if src == nil {
			return fmt.Errorf("converting NULL to %s is unsupported", dv.Kind())
		}
		switch v := src.(type) {
		case string:
			dv.SetString(v)
			return nil
		case []byte:
			dv.SetString(string(v))
			return nil
		}
	}

	return fmt.Errorf("unsupported Scan, storing driver.Value type %T into type %T", src, dest)
}

func cloneBytes(b []byte) []byte {
	if b == nil {
		return nil
	}
	c := make([]byte, len(b))
	copy(c, b)
	return c
}

func asString(src interface{}) string {
	switch v := src.(type) {
	case string:
		return v
	case []byte:
		return string(v)
	}
	rv := reflect.ValueOf(src)
	switch rv.Kind() {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return strconv.FormatInt(rv.Int(), 10)
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return strconv.FormatUint(rv.Uint(), 10)
	case reflect.Float64:
		return strconv.FormatFloat(rv.Float(), 'g', -1, 64)
	case reflect.Float32:
		return strconv.FormatFloat(rv.Float(), 'g', -1, 32)
	case reflect.Bool:
		return strconv.FormatBool(rv.Bool())
	}
	return fmt.Sprintf("%v", src)
}

func strconvErr(err error) error {
	if ne, ok := err.(*strconv.NumError); ok {
		return ne.Err
	}
	return err
}

func asBytes(buf []byte, rv reflect.Value) (b []byte, ok bool) {
	switch rv.Kind() {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return strconv.AppendInt(buf, rv.Int(), 10), true
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return strconv.AppendUint(buf, rv.Uint(), 10), true
	case reflect.Float32:
		return strconv.AppendFloat(buf, rv.Float(), 'g', -1, 32), true
	case reflect.Float64:
		return strconv.AppendFloat(buf, rv.Float(), 'g', -1, 64), true
	case reflect.Bool:
		return strconv.AppendBool(buf, rv.Bool()), true
	case reflect.String:
		s := rv.String()
		return append(buf, s...), true
	}
	return
}

/******************************************************************************
*                           Time related utils                                *
******************************************************************************/

func parseDateTime(b []byte, loc *time.Location) (time.Time, error) {
	const base = "0000-00-00 00:00:00.000000"
	switch len(b) {
	case 10, 19, 21, 22, 23, 24, 25, 26: // up to "YYYY-MM-DD HH:MM:SS.MMMMMM"
		if string(b) == base[:len(b)] {
			return time.Time{}, nil
		}

		year, err := parseByteYear(b)
		if err != nil {
			return time.Time{}, err
		}
		if year <= 0 {
			year = 1
		}

		if b[4] != '-' {
			return time.Time{}, fmt.Errorf("bad value for field: `%c`", b[4])
		}

		m, err := parseByte2Digits(b[5], b[6])
		if err != nil {
			return time.Time{}, err
		}
		if m <= 0 {
			m = 1
		}
		month := time.Month(m)

		if b[7] != '-' {
			return time.Time{}, fmt.Errorf("bad value for field: `%c`", b[7])
		}

		day, err := parseByte2Digits(b[8], b[9])
		if err != nil {
			return time.Time{}, err
		}
		if day <= 0 {
			day = 1
		}
		if len(b) == 10 {
			return time.Date(year, month, day, 0, 0, 0, 0, loc), nil
		}

		if b[10] != ' ' {
			return time.Time{}, fmt.Errorf("bad value for field: `%c`", b[10])
		}

		hour, err := parseByte2Digits(b[11], b[12])
		if err != nil {
			return time.Time{}, err
		}
		if b[13] != ':' {
			return time.Time{}, fmt.Errorf("bad value for field: `%c`", b[13])
		}

		min, err := parseByte2Digits(b[14], b[15])
		if err != nil {
			return time.Time{}, err
		}
		if b[16] != ':' {
			return time.Time{}, fmt.Errorf("bad value for field: `%c`", b[16])
		}

		sec, err := parseByte2Digits(b[17], b[18])
		if err != nil {
			return time.Time{}, err
		}
		if len(b) == 19 {
			return time.Date(year, month, day, hour, min, sec, 0, loc), nil
		}

		if b[19] != '.' {
			return time.Time{}, fmt.Errorf("bad value for field: `%c`", b[19])
		}
		nsec, err := parseByteNanoSec(b[20:])
		if err != nil {
			return time.Time{}, err
		}
		return time.Date(year, month, day, hour, min, sec, nsec, loc), nil
	default:
		return time.Time{}, fmt.Errorf("invalid time bytes: %s", b)
	}
}

func parseByteYear(b []byte) (int, error) {
	year, n := 0, 1000
	for i := 0; i < 4; i++ {
		v, err := bToi(b[i])
		if err != nil {
			return 0, err
		}
		year += v * n
		n = n / 10
	}
	return year, nil
}

func parseByte2Digits(b1, b2 byte) (int, error) {
	d1, err := bToi(b1)
	if err != nil {
		return 0, err
	}
	d2, err := bToi(b2)
	if err != nil {
		return 0, err
	}
	return d1*10 + d2, nil
}

func parseByteNanoSec(b []byte) (int, error) {
	ns, digit := 0, 100000 // max is 6-digits
	for i := 0; i < len(b); i++ {
		v, err := bToi(b[i])
		if err != nil {
			return 0, err
		}
		ns += v * digit
		digit /= 10
	}
	// nanoseconds has 10-digits. (needs to scale digits)
	// 10 - 6 = 4, so we have to multiple 1000.
	return ns * 1000, nil
}

func bToi(b byte) (int, error) {
	if b < '0' || b > '9' {
		return 0, errors.New("not [0-9]")
	}
	return int(b - '0'), nil
}
