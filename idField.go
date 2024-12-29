package csvParse

import (
	"fmt"
	"reflect"
	"strings"
)

type IdField struct {
	// a grouping of parameters tht can be any depth into the data
	Parameters []IdFieldParameter

	Delimiter string
}

type IdFieldParameter struct {
	Mapping []any
}

// Processes data and appends the ID field to all objects in the array
func (i *IdField) Process(data []map[string]any) ([]string, error) {
	if len(data) == 0 {
		return nil, fmt.Errorf("data is empty")
	} else if len(i.Parameters) == 0 {
		return nil, fmt.Errorf("parameters should not be empty")
	}

	ids := make([]string, len(data))

	for n := range data {
		var (
			idField strings.Builder
			first   = true
		)

		for _, parameter := range i.Parameters {
			if !first {
				idField.WriteString(i.Delimiter)
			}
			first = false

			result, err := getParameter(data[n], parameter.Mapping)
			if err != nil {
				return nil, fmt.Errorf("error finding result for mapping %s: %w", parameter.Mapping, err)
			}
			idField.WriteString(result)
		}

		ids[n] = idField.String()
	}

	return ids, nil
}

func getParameter(data map[string]any, targetMapping []any) (result string, err error) {
	current := reflect.ValueOf(data)
	for _, param := range targetMapping {
		paramValue := reflect.ValueOf(param)
		if current.Kind() == reflect.Interface {
			current = current.Elem()
		}

		switch current.Kind() {
		case reflect.Map:
			mapKeys := current.MapKeys()
			if len(mapKeys) == 0 {
				return "", fmt.Errorf("length of target map is 0")
			}

			if paramValue.Kind() != mapKeys[0].Kind() {
				return "", fmt.Errorf("expected %s key for map, got '%s'", mapKeys[0].Kind(), paramValue.Kind())
			}
			current = current.MapIndex(paramValue)
			if !current.IsValid() {
				return "", fmt.Errorf("key '%s' not found in map", paramValue)
			}

		case reflect.Slice, reflect.Array:
			if paramValue.Kind() != reflect.Int {
				return "", fmt.Errorf("expected integer index for array, got '%s'", paramValue.Kind())
			}
			index := int(paramValue.Int())
			if index < 0 || index >= current.Len() {
				return "", fmt.Errorf("index '%d' out of bounds", index)
			}
			current = current.Index(index)
		default:
			return "", fmt.Errorf("unexpected type '%s' while processing parameter", current.Kind())
		}
	}

	// Append the resolved ID to the map with the key '_id'
	if current.IsValid() {
		return fmt.Sprintf("%v", current.Interface()), nil
	} else {
		return "", fmt.Errorf("failed to resolve ID")
	}
}
