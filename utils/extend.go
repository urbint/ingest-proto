package utils

import "reflect"

// Extend copies non-zero values from src
// to dest
//
// It returns dest
func Extend(dest interface{}, src interface{}) interface{} {
	destValue := reflect.Indirect(reflect.ValueOf(dest))
	srcValue := reflect.Indirect(reflect.ValueOf(src))

	srcType := srcValue.Type()

	for i := 0; i < srcType.NumField(); i++ {
		srcField := srcType.Field(i)
		srcFieldValue := srcValue.Field(i)

		// Skip anonymous and Zero value fields
		if !srcFieldValue.CanInterface() || reflect.DeepEqual(reflect.Zero(srcField.Type).Interface(), srcFieldValue.Interface()) {
			continue
		}

		destField := destValue.FieldByName(srcField.Name)
		if !destField.CanSet() {
			continue
		}

		destField.Set(srcFieldValue)
	}

	return dest
}
