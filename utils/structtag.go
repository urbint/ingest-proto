package utils

import (
	"reflect"
	"strings"
)

// MapFromStructTag builds a hash map using the string specified by tagName as the keys
//
// Optionally omitempty may be added to the value to cause a zero value of the field to be
// omitted from the map entirely
func MapFromStructTag(src interface{}, tagName string) map[string]interface{} {
	result := map[string]interface{}{}

	srcValue := reflect.ValueOf(src)
	srcType := reflect.Indirect(srcValue).Type()

	for i := 0; i < srcType.NumField(); i++ {
		srcStructField := srcType.Field(i)
		tagValues := strings.Split(srcStructField.Tag.Get(tagName), ",")

		if tagValues[0] == "-" {
			continue
		}

		outName := tagValues[0]
		if outName == "" {
			outName = srcStructField.Name
		}

		var omitEmpty bool
		if len(tagValues) > 1 && tagValues[1] == "omitempty" {
			omitEmpty = true
		}

		fieldVal := reflect.Indirect(srcValue).Field(i)

		iFieldVal := fieldVal.Interface()

		if omitEmpty && reflect.DeepEqual(reflect.Zero(srcStructField.Type).Interface(), iFieldVal) {
			continue
		} else if reflect.Indirect(fieldVal).Type().Kind() == reflect.Struct {
			result[outName] = MapFromStructTag(fieldVal.Interface(), tagName)
		} else {
			result[outName] = iFieldVal
		}
	}

	return result
}
