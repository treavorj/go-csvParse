package csvParse

import (
	"fmt"
	"strconv"
	"strings"
	"time"
)

type Float64 float64 // Custom type to handle mapping with decimals

func (f Float64) MarshalJSON() ([]byte, error) {
	str := strconv.FormatFloat(float64(f), 'f', -1, 64)
	if !strings.Contains(str, ".") {
		str += ".0"
	}
	return []byte(str), nil
}

// Type mapping
type DataType int

const (
	DataTypeAuto  DataType = iota // Infers data type from data
	DataTypeSplit                 // Creates a column with the type appended to the name. Is treated like auto on everything but tables
	DataTypeString
	DataTypeInt64
	DataTypeFloat64
	DataTypeBool
	DataTypeDateTimeStyle0 // Assumes local time | YYYY-MM-DD HH:MM:SS
	DataTypeDateTimeStyle1 // Assumes local time | YYYY/MM/DD HH:MM:SS
)

func (dt DataType) String() string {
	switch dt {
	case DataTypeSplit:
		return "split"
	case DataTypeString:
		return "string"
	case DataTypeInt64:
		return "int64"
	case DataTypeFloat64:
		return "Float64"
	case DataTypeBool:
		return "bool"
	case DataTypeAuto:
		return "auto"
	case DataTypeDateTimeStyle0:
		return "2006-01-02 15:04:05"
	case DataTypeDateTimeStyle1:
		return "2006/01/02 15:04:05"
	default:
		return "unknown"
	}
}

func (dt DataType) Read(value string) (any, error) {
	switch dt {
	case DataTypeAuto:
		return dt.readAuto(value), nil
	case DataTypeSplit:
		return dt.readAuto(value), nil
	case DataTypeString:
		return value, nil
	case DataTypeInt64:
		return strconv.ParseInt(value, 10, 64)
	case DataTypeFloat64:
		val, err := strconv.ParseFloat(value, 64)
		return Float64(val), err
	case DataTypeBool:
		data, err := strconv.ParseBool(value)
		if err == nil {
			return data, nil
		}
		switch strings.ToLower(value) {
		case "ok":
			return true, nil
		case "ng":
			return false, nil
		default:
			return nil, nil
		}
	case DataTypeDateTimeStyle0:
		return dt.readDate(value)
	case DataTypeDateTimeStyle1:
		return dt.readDate(value)
	default:
		return nil, fmt.Errorf("unknown data type: %d", dt)
	}
}

func (dt *DataType) readDate(value string) (result any, err error) {
	data, err := time.ParseInLocation(dt.String(), value, time.Local)
	if err != nil {
		return nil, fmt.Errorf("failed to parse date: %w", err)
	}
	return data.Format(time.RFC3339), nil
}

func (dt *DataType) readNumber(value string, defaultToFloat bool) (result any, err error) {
	if !defaultToFloat {
		// check for integer
		result, err = strconv.ParseInt(value, 10, 64)
		if err == nil {
			return result.(int64), nil
		}
	}

	// check for float
	result, err = strconv.ParseFloat(value, 64)
	if err == nil {
		return Float64(result.(float64)), nil
	}

	// Unable to convert to number
	return nil, fmt.Errorf("unable to convert to number: %s", value)
}

// Sets the data to the right type you must assert the type of data
func (dt *DataType) readAuto(value string) any {
	// check if blank
	if value == "" {
		return nil
	}

	// check for number
	result, err := dt.readNumber(value, true)
	if err == nil {
		return result
	}

	// check for bool
	result, err = strconv.ParseBool(value)
	if err == nil {
		return result.(bool)
	}

	// default to string
	return value
}
