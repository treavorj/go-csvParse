package csvParse

import (
	"fmt"
	"strings"
)

// Represents cell data that spans multiple consecutive rows or columns
type ConcatCellLocation struct {
	Cells     []Cell // List of cells in order to be concatenated
	Delimiter string // Delimiter to use between cells
	Name      string // Alias that will be used if not blank
	NameCell  Cell   // Location for the name cell. Will be ignored if Name is not blank
	DataType  DataType
}

func NewConcatCellLocation(cells []Cell, delimiter string, name string, nameCell Cell) (*ConcatCellLocation, error) {
	if name == "" && nameCell == (Cell{}) {
		return nil, fmt.Errorf("either name or nameCell must be provided")
	}
	return &ConcatCellLocation{
		Cells:     cells,
		Delimiter: delimiter,
		Name:      name,
		NameCell:  nameCell,
	}, nil
}

// parses and concatenates multiple cells information from records of a csv file
func (c *ConcatCellLocation) Parse(records [][]string) (string, any, error) {
	var err error
	name := c.Name
	if name == "" {
		name, err = findValue(c.NameCell, records)
		if err != nil {
			return "", nil, fmt.Errorf("error finding name for cell: %w", err)
		}
	}

	var values []string
	for _, cell := range c.Cells {
		value, err := findValue(cell, records)
		if err != nil {
			return "", nil, fmt.Errorf("error finding value for cell (%d, %d): %w", cell.Row, cell.Column, err)
		}
		values = append(values, value)
	}

	value := strings.Join(values, c.Delimiter)

	data, err := c.DataType.Read(value)
	if err != nil {
		return "", nil, fmt.Errorf("error converting value to data type; %w", err)
	}

	return name, data, nil
}
