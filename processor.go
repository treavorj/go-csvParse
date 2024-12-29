package csvParse

import (
	"encoding/json"
	"fmt"
	"strings"
)

type ProcessorType int

const (
	ProcessorTypeMergeColumns ProcessorType = iota
	ProcessorTypeMergeRows
	ProcessorTypeFillRight
	ProcessorTypeReplaceCell
	ProcessorTypeTransposeRow
	ProcessorTypeRemoveCellLeft

	ProcessorTypeEnd // Used to confirm all types are accounted for
)

func getProcessor(processorType ProcessorType, data json.RawMessage) (processor Processor, err error) {
	switch processorType {
	case ProcessorTypeMergeColumns:
		processor = &ProcessorMergeColumns{}
	case ProcessorTypeMergeRows:
		processor = &ProcessorMergeRows{}
	case ProcessorTypeFillRight:
		processor = &ProcessorFillRight{}
	case ProcessorTypeReplaceCell:
		processor = &ProcessorReplaceCell{}
	case ProcessorTypeTransposeRow:
		processor = &ProcessorTransposeRow{}
	case ProcessorTypeRemoveCellLeft:
		processor = &ProcessorRemoveCellLeft{}
	default:
		return nil, fmt.Errorf("invalid type: %d", processorType)
	}

	if err := json.Unmarshal(data, &processor); err != nil {
		return nil, fmt.Errorf("unable to unmarshal processorType %d: %w", processor.GetType(), err)
	}

	return processor, nil
}

type Processor interface {
	GetName() string
	Execute(records [][]string) ([][]string, error)
	GetType() ProcessorType
	SetType()
}

// # Merge one or more columns together across defined rows
//
// Example:
//
//	input := [][]string{
//		{"r0c0", "r0c1", "r0c2"},
//		{"r1c0", "r1c1", "r1c2", "r1c3"},
//		{"r2c0", "r2c1", "r2c2"},
//		{"r3c0", "r3c1", "r3c2"},
//		{"r4c0", "r4c1", "r4c2"},
//	}
//
//	action:= &ProcessorMergeColumns{
//		Start: Cell{Row: 2, Column: 0},
//		End:   Cell{Row: 4, Column: 2},
//	}
//
//	output := [][]string{
//		{"r0c0", "r0c1", "r0c2"},
//		{"r1c0", "r1c1", "r1c2", "r1c3"},
//		{"r2c0r2c1", "r2c2"},
//		{"r3c0r3c1", "r3c2"},
//		{"r4c0", "r4c1", "r4c2"},
//	}
type ProcessorMergeColumns struct {
	Type      ProcessorType
	Name      string
	Start     Cell
	End       Cell // Set Row = -1 if you want to do all rows, Set Column = -1 if you want to do all columns
	Delimiter string
}

func (a *ProcessorMergeColumns) GetName() string {
	return a.Name
}

func (a *ProcessorMergeColumns) GetType() ProcessorType {
	return ProcessorTypeMergeColumns
}

func (a *ProcessorMergeColumns) SetType() {
	a.Type = a.GetType()
}

func (a *ProcessorMergeColumns) Execute(records [][]string) ([][]string, error) {
	if a.Start.Row < 0 || a.Start.Column < 0 {
		return nil, fmt.Errorf("neither start row (%d) nor column (%d) can be < 0", a.Start.Row, a.Start.Column)
	} else if a.End.Row < a.Start.Row && !(a.End.Row < 0) {
		return nil, fmt.Errorf("end row (%d) cannot be less than start row (%d) unless less than zero", a.End.Row, a.Start.Row)
	} else if a.End.Column < a.Start.Column && !(a.End.Column < 0) {
		return nil, fmt.Errorf("end column (%d) cannot be less than start column (%d) unless less than zero", a.End.Column, a.Start.Column)
	} else if a.Start.Row > len(records) {
		return nil, fmt.Errorf("start row (%d) cannot be greater than the length of records (%d)", a.Start.Row, len(records))
	} else if a.End.Row > len(records) {
		return nil, fmt.Errorf("end row (%d) cannot be greater than the length of records (%d)", a.End.Row, len(records))
	}

	endRow := a.End.Row
	if endRow < 0 {
		endRow = len(records)
	}
	endColumn := a.End.Column

	new := make([][]string, len(records))
	for n := 0; n < a.Start.Row; n++ {
		new[n] = records[n]
	}

	for n := a.Start.Row; n < endRow; n++ {
		if a.End.Column < 0 {
			endColumn = len(records[n])
		}
		if endColumn > len(records[n]) {
			return nil, fmt.Errorf("row %d: endColumn (%d) cannot be greater than the length of columns (%d)", n, endColumn, len(records[n]))
		}

		new[n] = make([]string, len(records[n])-(endColumn-a.Start.Column)+1)
		if a.Start.Column != 0 {
			copy(new[n], records[n][:a.Start.Column])
		}

		new[n][a.Start.Column] = strings.Join(records[n][a.Start.Column:endColumn], a.Delimiter)

		if endColumn < len(records[n]) {
			copy(new[n][a.Start.Column+1:], records[n][endColumn:])
		}
	}

	for n := endRow; n < len(records); n++ {
		new[n] = records[n]
	}

	return new, nil
}

// # Merge one or more rows together.
//
// Note: All rows must be the same length
//
// Example:
//
//	input := [][]string{
//		{"r0c0", "r0c1", "r0c2"},
//		{"r1c0", "r1c1", "r1c2", "r1c3"},
//		{"r2c0", "r2c1", "r2c2"},
//		{"r3c0", "r3c1", "r3c2"},
//		{"r4c0", "r4c1", "r4c2"},
//	}
//	action:= &ProcessorMergeRows{
//	  StartRow: 2,
//	  EndRow: 4,
//	}
//	output := [][]string{
//		{"r0c0", "r0c1", "r0c2"},
//		{"r1c0", "r1c1", "r1c2", "r1c3"},
//		{"r2c0r3c0", "r2c1r3c1", "r2c2r3c2"},
//		{"r4c0", "r4c1", "r4c2"},
//	}
type ProcessorMergeRows struct {
	Type           ProcessorType
	Name           string
	StartRow       int
	EndRow         int
	Delimiter      string
	TrimWhitespace bool
}

func (a *ProcessorMergeRows) GetName() string {
	return a.Name
}
func (a *ProcessorMergeRows) GetType() ProcessorType {
	return ProcessorTypeMergeRows
}
func (a *ProcessorMergeRows) SetType() {
	a.Type = a.GetType()
}

func (a *ProcessorMergeRows) Execute(records [][]string) ([][]string, error) {
	if a.StartRow < 0 {
		return nil, fmt.Errorf("neither start row (%d) can be < 0", a.StartRow)
	} else if a.EndRow < a.StartRow && !(a.EndRow < 0) {
		return nil, fmt.Errorf("end row (%d) cannot be less than start row (%d) unless less than zero", a.EndRow, a.StartRow)
	} else if a.StartRow > len(records) {
		return nil, fmt.Errorf("start row (%d) cannot be greater than the length of records (%d)", a.StartRow, len(records))
	} else if a.EndRow > len(records) {
		return nil, fmt.Errorf("end row (%d) cannot be greater than the length of records (%d)", a.EndRow, len(records))
	}

	endRow := a.EndRow
	if endRow < 0 {
		endRow = len(records)
	}

	length := len(records) - (a.EndRow - a.StartRow) + 1
	if a.EndRow < 0 {
		length = 1 + a.StartRow
	}

	new := make([][]string, length)
	for n := 0; n < a.StartRow; n++ {
		new[n] = records[n]
	}

	new[a.StartRow] = make([]string, len(records[a.StartRow]))
	copy(new[a.StartRow], records[a.StartRow])
	for n := a.StartRow + 1; n < endRow; n++ {
		if len(new[a.StartRow]) != len(records[n]) {
			return nil, fmt.Errorf("all merged rows must be the same length. Row %d length (%d) does not match other rows (%d)", n, len(records[n]), len(new[a.StartRow]))
		}
		if a.TrimWhitespace {
			for i := 0; i < len(records[n]); i++ {
				new[a.StartRow][i] = strings.TrimSpace(strings.Join([]string{strings.TrimSpace(new[a.StartRow][i]), strings.TrimSpace(records[n][i])}, a.Delimiter))
			}
		} else {
			for i := 0; i < len(records[n]); i++ {
				new[a.StartRow][i] = strings.Join([]string{new[a.StartRow][i], records[n][i]}, a.Delimiter)
			}
		}
	}

	for n := endRow; n < len(records); n++ {
		new[a.StartRow+(n-endRow+1)] = records[n]
	}

	return new, nil
}

// # Fills blank spaces using data to the left of the blank column
//
// Example:
//
//	input := [][]string{
//		{"r0c0", "r0c1", "r0c2"},
//		{"r1c0", "r1c1", "r1c2", "r1c3"},
//		{"r2c0", "r2c1", , , "r2c2"},
//		{"r3c0", "r3c1", , , "r3c2"},
//		{"r4c0", "r4c1", , , "r4c2"},
//	}
//	action:= &ProcessorFillRight{
//	  Start: Cell{Row: 2, Column: 0},
//	  End: Cell{Row: 4, Column: 4},
//	}
//	output := [][]string{
//		{"r0c0", "r0c1", "r0c2"},
//		{"r1c0", "r1c1", "r1c2", "r1c3"},
//		{"r2c0", "r2c1", "r2c1", , "r2c2"},
//		{"r3c0", "r3c1", "r3c1", , "r3c2"},
//		{"r4c0", "r4c1", , , "r4c2"},
//	}
type ProcessorFillRight struct {
	Type  ProcessorType
	Name  string
	Start Cell
	End   Cell // Set Row = -1 if you want to do all rows, Set Column = -1 if you want to do all columns
}

func (a *ProcessorFillRight) GetName() string {
	return a.Name
}
func (a *ProcessorFillRight) GetType() ProcessorType {
	return ProcessorTypeFillRight
}
func (a *ProcessorFillRight) SetType() {
	a.Type = a.GetType()
}

func (a *ProcessorFillRight) Execute(records [][]string) ([][]string, error) {
	if a.Start.Row < 0 || a.Start.Column < 0 {
		return nil, fmt.Errorf("neither start row (%d) nor column (%d) can be < 0", a.Start.Row, a.Start.Column)
	} else if a.End.Row < a.Start.Row && !(a.End.Row < 0) {
		return nil, fmt.Errorf("end row (%d) cannot be less than start row (%d) unless less than zero", a.End.Row, a.Start.Row)
	} else if a.End.Column < a.Start.Column && !(a.End.Column < 0) {
		return nil, fmt.Errorf("end column (%d) cannot be less than start column (%d) unless less than zero", a.End.Column, a.Start.Column)
	} else if a.Start.Row > len(records) {
		return nil, fmt.Errorf("start row (%d) cannot be greater than the length of records (%d)", a.Start.Row, len(records))
	} else if a.End.Row > len(records) {
		return nil, fmt.Errorf("end row (%d) cannot be greater than the length of records (%d)", a.End.Row, len(records))
	}

	endRow := a.End.Row
	if endRow < 0 {
		endRow = len(records)
	}
	endColumn := a.End.Column

	for n := a.Start.Row; n < endRow; n++ {
		if a.End.Column < 0 {
			endColumn = len(records[n])
		}
		if endColumn > len(records[n]) {
			return nil, fmt.Errorf("row %d: endColumn (%d) cannot be greater than the length of columns (%d)", n, endColumn, len(records[n]))
		}

		for i := a.Start.Column; i < endColumn; i++ {
			if strings.TrimSpace(records[n][i]) != "" {
				continue
			} else if i == 0 {
				continue
			} else {
				records[n][i] = records[n][i-1]
			}
		}
	}

	return records, nil
}

// # Replaces a cell's value with another
//
// Example:
//
//	input := [][]string{
//		{"r0c0", "r0c1", "r0c2"},
//		{"r1c0", "r1c1", "r1c2"},
//	}
//	action:= &ProcessorFillRight{
//	  Cell: Cell{Row: 0, Column: 0},
//	  Value: "header1",
//	}
//	output := [][]string{
//		{"header1", "r0c1", "r0c2"},
//		{"r1c0", "r1c1", "r1c2"},
//	}
type ProcessorReplaceCell struct {
	Type  ProcessorType
	Name  string
	Cell  Cell
	Value string
}

func (a *ProcessorReplaceCell) GetName() string {
	return a.Name
}
func (a *ProcessorReplaceCell) GetType() ProcessorType {
	return ProcessorTypeReplaceCell
}
func (a *ProcessorReplaceCell) SetType() {
	a.Type = a.GetType()
}

func (a *ProcessorReplaceCell) Execute(records [][]string) ([][]string, error) {
	if a.Cell.Row < 0 || a.Cell.Column < 0 {
		return nil, fmt.Errorf("neither cell row (%d) nor column (%d) can be < 0", a.Cell.Row, a.Cell.Column)
	} else if a.Cell.Row > len(records) {
		return nil, fmt.Errorf("cell row (%d) cannot be greater than the length of records (%d)", a.Cell.Row, len(records))
	} else if a.Cell.Column > len(records[a.Cell.Row]) {
		return nil, fmt.Errorf("cell row (%d) cannot be greater than the length of records row (%d)", a.Cell.Row, len(records[a.Cell.Row]))
	}

	records[a.Cell.Row][a.Cell.Column] = a.Value
	return records, nil
}

// # Transpose data from horizontal to vertical or vice versa
//
// Example:
//
//		input := [][]string{
//			{"r0c0", "r0c1", "r0c2"},
//			{"r1c0", "r1c1", "r1c2"},
//		}
//		action:= &ProcessorTransposeRow{
//		  StartRow: 0,
//	   EndRow: -1
//		}
//		output := [][]string{
//			{"r0c0", "r1c0"},
//			{"r0c1", "r1c1"},
//			{"r0c2", "r1c2"},
//		}
type ProcessorTransposeRow struct {
	Type     ProcessorType
	Name     string
	StartRow int
	EndRow   int
}

func (a *ProcessorTransposeRow) GetName() string {
	return a.Name
}
func (a *ProcessorTransposeRow) GetType() ProcessorType {
	return ProcessorTypeTransposeRow
}
func (a *ProcessorTransposeRow) SetType() {
	a.Type = a.GetType()
}

func (a *ProcessorTransposeRow) Execute(records [][]string) ([][]string, error) {
	if a.StartRow < 0 {
		return nil, fmt.Errorf("neither start row (%d) can be < 0", a.StartRow)
	} else if a.EndRow < a.StartRow && !(a.EndRow < 0) {
		return nil, fmt.Errorf("end row (%d) cannot be less than start row (%d) unless less than zero", a.EndRow, a.StartRow)
	} else if a.StartRow > len(records) {
		return nil, fmt.Errorf("start row (%d) cannot be greater than the length of records (%d)", a.StartRow, len(records))
	} else if a.EndRow > len(records) {
		return nil, fmt.Errorf("end row (%d) cannot be greater than the length of records (%d)", a.EndRow, len(records))
	}

	endRow := a.EndRow
	if endRow < 0 {
		endRow = len(records)
	}

	length := a.StartRow + len(records[a.StartRow])
	if a.EndRow > 0 {
		length += len(records) - a.EndRow
	}

	new := make([][]string, length)
	for n := 0; n < a.StartRow; n++ {
		new[n] = records[n]
	}

	first := true
	for n := a.StartRow; n < endRow; n++ {
		if len(records[a.StartRow]) != len(records[n]) {
			return nil, fmt.Errorf("all transposed rows must be the same length. Row %d length (%d) does not match other rows (%d)", n, len(records[n]), len(new[a.StartRow]))
		}

		for i := 0; i < len(records[n]); i++ {
			if first {
				new[n+i] = make([]string, endRow-a.StartRow)
			}
			new[a.StartRow+i][n-a.StartRow] = records[n][i]
		}
		first = false
	}

	for n := endRow; n < len(records); n++ {
		new[a.StartRow+len(records[a.StartRow])+n-endRow] = records[n]
	}

	return new, nil
}

// # Removes a cell and shifts left
//
// Example:
//
//	input := [][]string{
//		{"r0c0", "r0c1", "r0c2"},
//		{"r1c0", "r1c1", "r1c2"},
//	}
//	action:= &ProcessorFillRight{
//	  Cell: Cell{Row: 0, Column: 0},
//	}
//	output := [][]string{
//		{"r0c1", "r0c2"},
//		{"r1c0", "r1c1", "r1c2"},
//	}
type ProcessorRemoveCellLeft struct {
	Type ProcessorType
	Name string
	Cell Cell
}

func (a *ProcessorRemoveCellLeft) GetName() string {
	return a.Name
}
func (a *ProcessorRemoveCellLeft) GetType() ProcessorType {
	return ProcessorTypeRemoveCellLeft
}
func (a *ProcessorRemoveCellLeft) SetType() {
	a.Type = a.GetType()
}

func (a *ProcessorRemoveCellLeft) Execute(records [][]string) ([][]string, error) {
	if a.Cell.Row < 0 || a.Cell.Column < 0 {
		return nil, fmt.Errorf("neither cell row (%d) nor column (%d) can be < 0", a.Cell.Row, a.Cell.Column)
	} else if a.Cell.Row > len(records) {
		return nil, fmt.Errorf("cell row (%d) cannot be greater than the length of records (%d)", a.Cell.Row, len(records))
	} else if a.Cell.Column > len(records[a.Cell.Row]) {
		return nil, fmt.Errorf("cell row (%d) cannot be greater than the length of records row (%d)", a.Cell.Row, len(records[a.Cell.Row]))
	}

	newRow := make([]string, len(records[a.Cell.Row])-1)
	deleted := 0
	for i := 0; i < len(records[a.Cell.Row])-1; i++ {
		if i == a.Cell.Column {
			deleted = 1
		}
		newRow[i] = records[a.Cell.Row][i+deleted]
	}

	records[a.Cell.Row] = newRow

	return records, nil
}
