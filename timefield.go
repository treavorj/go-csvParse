package csvParse

import (
	"fmt"
	"strings"
	"time"
)

type TimeField struct {
	Cells  []Cell // List of cells. Can be concatenated if the time field spans multiple areas
	Layout string
	Name   string
}

func NewTimeField(cells []Cell, Layout string) *TimeField {
	return &TimeField{
		Cells:  cells,
		Layout: Layout,
	}
}

func (t *TimeField) Parse(records [][]string) (time.Time, error) {
	var timeStr strings.Builder
	for _, cell := range t.Cells {
		value, err := findValue(cell, records)
		if err != nil {
			return time.Time{}, fmt.Errorf("error finding value for cell (%d, %d): %w", cell.Row, cell.Column, err)
		}
		timeStr.WriteString(value)
	}

	timestamp, err := time.ParseInLocation(t.Layout, timeStr.String(), time.Local)
	if err != nil {
		return time.Time{}, fmt.Errorf("error converting value (%s) to the specified layout (%s): %w", timeStr.String(), t.Layout, err)
	}
	return timestamp, nil
}

func (t *TimeField) IsBlank() bool {
	if len(t.Layout) == 0 {
		return true
	} else if len(t.Cells) == 0 {
		return true
	}

	return false
}
