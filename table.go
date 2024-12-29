package csvParse

import (
	"fmt"
	"strings"
)

// Represents data that is within a table.
type TableLocation struct {
	Name                string // name of the table. will be used instead of NameLocation if both are provided
	NameLocation        Cell   // if not 0, 0 it will be used to identify the name of the table
	StartCell           Cell
	EndCell             Cell       // if 0, 0 or equal to start cell it will not execute. Negative values will be treated as the end of the row or column
	HeaderNames         []string   // Ignored if TableHasHeader is true
	ColumnDataTypes     []DataType // Ignored if AutoColumnDataTypes is true.
	TableHasHeader      bool       // Whether the first row is the header row or not
	AutoColumnDataTypes bool       // if true will automatically infer column data types from the data
	SkipBlankData       bool       // Skips returning data for a cell if the cell is blank
	ParseAsArray        bool       // If true will parse the fields as an array instead of a JSON list
	ParseSingleRow      bool       // If true will only take the first row (or row beneath header) regardless of number of rows
	ParseSeparated      bool       // If true will segment table into multiple maps
	IgnoreNesting       bool       // If true will not nest the fields under the table name
}

func NewTableLocation(
	name string,
	nameLocation Cell,
	startCell Cell,
	endCell Cell,
	tableHasHeader bool,
	headerNames []string,
	autoColumnDataTypes bool,
	columnDataTypes []DataType,
	skipBlankData bool,
) (*TableLocation, error) {
	if name != "" && nameLocation != (Cell{}) {
		return nil, fmt.Errorf("only one of name or nameLocation should be provided")
	}
	if name == "" && nameLocation == (Cell{}) {
		return nil, fmt.Errorf("either name or nameLocation must be provided")
	}
	if startCell == endCell {
		return nil, fmt.Errorf("startCell should not be equal to endCell")
	}
	if endCell.Column > 0 && endCell.Row > 0 && (startCell.Column > endCell.Column || startCell.Row > endCell.Row) {
		return nil, fmt.Errorf("startCell should be less than or equal to endCell")
	}
	if tableHasHeader && len(headerNames) > 0 {
		return nil, fmt.Errorf("headerRowNames should not be provided if TableHasHeader is true")
	}
	if len(headerNames) > 0 && int(endCell.Column-startCell.Column) != len(headerNames) {
		return nil, fmt.Errorf("headerRowNames should have the same number of columns as the table")
	}
	if autoColumnDataTypes && len(columnDataTypes) > 0 {
		return nil, fmt.Errorf("only one of autoColumnDataTypes or manualColumnDataTypes should be provided")
	}
	if !autoColumnDataTypes && len(columnDataTypes) == 0 {
		return nil, fmt.Errorf("either autoColumnDataTypes or manualColumnDataTypes should be provided")
	}
	if !tableHasHeader && !autoColumnDataTypes && len(headerNames) != len(columnDataTypes) {
		return nil, fmt.Errorf("manualColumnDataTypes should have the same number of columns as the table if TableHasHeader is false and autoColumnDataTypes is false")
	}

	return &TableLocation{
		Name:                name,
		NameLocation:        nameLocation,
		StartCell:           startCell,
		EndCell:             endCell,
		TableHasHeader:      tableHasHeader,
		HeaderNames:         headerNames,
		AutoColumnDataTypes: autoColumnDataTypes,
		ColumnDataTypes:     columnDataTypes,
		SkipBlankData:       skipBlankData,
	}, nil
}

// used to parse a given table based on a table location from csv records
func (t *TableLocation) Parse(records [][]string, keepSpaces bool) (string, any, error) {
	tableName, headers, tableDims, err := t.parseTableHeader(records, keepSpaces)
	if err != nil {
		return "", nil, fmt.Errorf("error parsing table header: %w", err)
	}

	var tableData any

	switch {
	case t.ParseAsArray:
		tableData, err = t.parseTableDataArray(tableDims, records, headers)
	case t.ParseSingleRow:
		tableData, err = t.parseTableSingleRow(tableDims, records, headers)
	default:
		tableData, err = t.parseTableData(tableDims, records, headers)
	}
	if err != nil {
		return *tableName, nil, fmt.Errorf("error parsing table %s: %w", *tableName, err)
	}
	return *tableName, tableData, nil
}

type tableDimensions struct {
	startRow    int
	endRow      int
	startColumn int
	endColumn   int
}

// helper function for parsing header data for tables
func (t *TableLocation) parseTableHeader(records [][]string, keepSpaces bool) (*string, []string, *tableDimensions, error) {
	var err error
	var tableDims tableDimensions

	// Find name of table
	tableName := t.Name
	if tableName == "" {
		tableName, err = findValue(t.NameLocation, records)
		if err != nil {
			return nil, nil, nil, fmt.Errorf("error finding name for table: %w", err)
		}
		if !keepSpaces {
			tableName = strings.ReplaceAll(tableName, " ", "_")
		}
	}

	// Find rows of table
	tableDims.startRow = t.StartCell.Row
	if t.EndCell.Row > 0 {
		tableDims.endRow = t.EndCell.Row
	} else {
		tableDims.endRow = len(records) - 1
	}

	// Find columns of table
	tableDims.startColumn = t.StartCell.Column
	if t.EndCell.Column > 0 {
		tableDims.endColumn = t.EndCell.Column
	} else {
		if len(records) <= t.StartCell.Row {
			return nil, nil, nil, fmt.Errorf("csv shorter (%d) than table start (%d)", len(records), t.StartCell.Row)
		}
		tableDims.endColumn = len(records[t.StartCell.Row]) - 1
	}

	// Parse Header
	var headers []string
	if t.TableHasHeader {
		headers = records[t.StartCell.Row][tableDims.startColumn : tableDims.endColumn+1]
		tableDims.startRow += 1
	} else {
		headers = t.HeaderNames
	}

	if !keepSpaces {
		for n := range headers {
			headers[n] = strings.ReplaceAll(headers[n], " ", "_")
		}
	}

	errorOnDuplicateName := false
	if errorOnDuplicateName {
		for _, header := range headers {
			if header == "title" {
				return &tableName, nil, nil, fmt.Errorf("")
			}
		}
	}
	return &tableName, headers, &tableDims, nil
}

// helper function which parses json style table
func (t *TableLocation) parseTableData(tableDims *tableDimensions, records [][]string, headers []string) ([]map[string]any, error) {
	tableData := make([]map[string]any, tableDims.endRow-tableDims.startRow+1)
	for row := tableDims.startRow; row <= tableDims.endRow; row++ {
		rowData := make(map[string]any)
		n := -1
		for column := tableDims.startColumn; column <= tableDims.endColumn; column++ {
			n++
			header := headers[n]
			rawData, err := findValue(Cell{Row: row, Column: column}, records)
			if err != nil {
				return nil, fmt.Errorf("error finding value for cell (%d, %d) with header (%v): %w", row, column, header, err)
			}
			dataType := DataTypeAuto
			if !t.AutoColumnDataTypes {
				dataType = t.ColumnDataTypes[n]
			}
			var data any
			data, err = dataType.Read(rawData)
			if err != nil {
				return nil, fmt.Errorf("error parsing data for cell (%d, %d) with header (%v): %w", row, column, header, err)
			}
			if data == nil && t.SkipBlankData {
				continue
			}
			if dataType == DataTypeSplit {
				header = fmt.Sprintf("%s_%T", header, data)
			}
			rowData[header] = data
		}
		tableData[row-tableDims.startRow] = rowData
	}
	return tableData, nil
}

// helper function which parses array style table
func (t *TableLocation) parseTableDataArray(tableDims *tableDimensions, records [][]string, headers []string) (map[string][]any, error) {
	tableData := make(map[string][]any)
	for column := tableDims.startColumn; column <= tableDims.endColumn; column++ {
		columnData := make([]any, tableDims.endRow-tableDims.startRow+1)
		dataType := DataTypeAuto
		if !t.AutoColumnDataTypes {
			dataType = t.ColumnDataTypes[column]
		}
		for row := tableDims.startRow; row <= tableDims.endRow; row++ {
			data, err := dataType.Read(records[row][column])
			if err != nil {
				return nil, fmt.Errorf("error parsing data for cell (%d, %d) with header (%s): %w", row, column, headers[column], err)
			}
			if data == nil && t.SkipBlankData {
				continue
			}
			columnData[row-tableDims.startRow] = data
		}
		tableData[headers[column]] = columnData
	}
	return tableData, nil
}

// helper function which returns only the first row of a table
func (t *TableLocation) parseTableSingleRow(tableDims *tableDimensions, records [][]string, headers []string) (map[string]any, error) {
	tableData := make(map[string]any, len(headers))
	for column := tableDims.startColumn; column <= tableDims.endColumn; column++ {
		dataType := DataTypeAuto
		if !t.AutoColumnDataTypes {
			dataType = t.ColumnDataTypes[column]
		}

		data, err := dataType.Read(records[tableDims.startRow][column])
		if err != nil {
			return nil, fmt.Errorf("error parsing data for cell (%d, %d) with header (%s): %w", tableDims.startRow, column, headers[column], err)
		}
		if data == nil && t.SkipBlankData {
			continue
		}

		tableData[headers[column]] = data
	}
	return tableData, nil
}
