package sdstore

import (
	"fmt"
	"reflect"
)

// isStruct returns true if v is a struct.
func isStruct(v any) bool {
	return reflect.ValueOf(v).Kind() == reflect.Struct
}

// isPointerToStruct returns true if v is a pointer to a struct.
func isPointerToStruct(v any) bool {
	t := reflect.TypeOf(v)
	if t.Kind() != reflect.Pointer {
		return false
	}

	return t.Elem().Kind() == reflect.Struct
}

// getFieldValue returns the value of a field if the provided struct has such a field.
//
// Returns nil if v is not a struct or pointer to struct or if v doesn't have the field
func getFieldValue(v any, field string) any {
	// Check if v is struct or *struct to prevent panics.
	if !isStruct(v) && !isPointerToStruct(v) {
		return nil
	}

	s := reflect.ValueOf(v)
	return s.FieldByName(field)
}

func key(field string, value any) string {
	return fmt.Sprintf("%s:%v", field, value)
}
