package csvParse

import "fmt"

type Cell struct {
	Row    int // start row is 0
	Column int // start column is 0
}

// Represents data that is within a single cell (not a table)
//
// The cell may contain an associated cell that is the name of
// the cell or the name can be supplied directly.
type CellLocation struct {
	Location Cell
	DataType DataType
	Name     string // Alias that will be used if not blank
	NameCell Cell   // Location for the name cell. Will be ignored if Name is not blank
}

func NewCellLocation(location Cell, dataType DataType, name string, nameCell Cell) (*CellLocation, error) {
	if name == "" && nameCell == (Cell{}) {
		return nil, fmt.Errorf("either name or nameCell must be provided")
	}
	return &CellLocation{
		Location: location,
		DataType: dataType,
		Name:     name,
		NameCell: nameCell,
	}, nil
}

// parses a cell's information from records of a csv file
func (c *CellLocation) Parse(records [][]string) (name string, data any, err error) {
	cellName := c.Name
	if cellName == "" {
		cellName, err = findValue(c.NameCell, records)
		if err != nil {
			return "", nil, fmt.Errorf("error finding name for cell: %w", err)
		}
	}

	value, err := findValue(c.Location, records)
	if err != nil {
		return "", nil, fmt.Errorf("error finding value for cell: %w", err)
	}
	cellData, err := c.DataType.Read(value)
	if err != nil {
		return "", nil, fmt.Errorf("error converting value to data type: %w", err)
	}
	return cellName, cellData, nil
}
