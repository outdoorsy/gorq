package plans

import "reflect"

// fieldByIndex is a copy of v.FieldByIndex, except that it will
// initialize nil pointers while descending the indexes.
func fieldByIndex(v reflect.Value, index []int) reflect.Value {
	for _, idx := range index {
		if v.Kind() == reflect.Ptr {
			if v.IsNil() {
				v.Set(reflect.New(v.Type().Elem()))
			}
			v = v.Elem()
		}
		switch v.Kind() {
		case reflect.Struct:
			v = v.Field(idx)
		default:
			panic("gorp: found unsupported type using fieldByIndex: " + v.Kind().String())
		}
	}
	return v
}

// fieldOrNilByIndex is like fieldByIndex, except that it performs no
// initialization.  If it finds a nil pointer, it just returns the nil
// pointer, even if it is not the field requested.
func fieldOrNilByIndex(v reflect.Value, index []int) reflect.Value {
	var f reflect.StructField
	t := v.Type()
	for _, idx := range index {
		if v.Kind() == reflect.Ptr {
			if v.IsNil() {
				return v
			}
			v = v.Elem()
			t = t.Elem()
		}
		v = v.Field(idx)
		f = t.Field(idx)
		t = f.Type
	}
	return v
}
