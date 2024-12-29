package csvParse

import (
	"reflect"
	"testing"
)

type testPreProcessor struct {
	input      [][]string
	output     [][]string
	action     Processor
	expectFail bool
}

func testCopyInput(input [][]string) [][]string {
	output := make([][]string, len(input))
	for i := range input {
		output[i] = make([]string, len(input[i]))
		copy(output[i], input[i])
	}
	return output
}

func TestProcessorType(t *testing.T) {
	t.Parallel()

	processorTypes := map[ProcessorType]int{
		ProcessorTypeMergeColumns:   0,
		ProcessorTypeMergeRows:      1,
		ProcessorTypeFillRight:      2,
		ProcessorTypeReplaceCell:    3,
		ProcessorTypeTransposeRow:   4,
		ProcessorTypeRemoveCellLeft: 5,

		ProcessorTypeEnd: 6,
	}

	if len(processorTypes)-1 != int(ProcessorTypeEnd) {
		t.Errorf("at least one type is positioned incorrectly or not included in this list")
	}

	for key, value := range processorTypes {
		if key != ProcessorType(value) {
			t.Errorf("ProcessorType %d does not equal fixed value of %d", key, value)
		}

		if value == int(ProcessorTypeEnd) {
			continue
		}

		processor, err := getProcessor(key, []byte(`{}`))
		if err != nil {
			t.Errorf("error trying to get the processor %d: %v", key, err)
		} else if processor == nil {
			t.Errorf("processor was nil")
		} else if processor.GetType() != key {
			t.Errorf("types do not match")
		}
	}
}

func TestProcessorMergeColumns(t *testing.T) {
	input := [][]string{
		{"r0c0", "r0c1", "r0c2"},
		{"r1c0", "r1c1", "r1c2", "r1c3"},
		{"r2c0", "r2c1", "r2c2"},
		{"r3c0", "r3c1", "r3c2"},
		{"r4c0", "r4c1", "r4c2"},
	}
	tests := []testPreProcessor{
		{
			input: testCopyInput(input),
			output: [][]string{
				{"r0c0", "r0c1", "r0c2"},
				{"r1c0", "r1c1", "r1c2", "r1c3"},
				{"r2c0r2c1", "r2c2"},
				{"r3c0r3c1", "r3c2"},
				{"r4c0", "r4c1", "r4c2"},
			},
			action: &ProcessorMergeColumns{
				Name:  "Test 0",
				Start: Cell{Row: 2, Column: 0},
				End:   Cell{Row: 4, Column: 2},
			},
		},
		{
			input: testCopyInput(input),
			output: [][]string{
				{"r0c0", "r0c1", "r0c2"},
				{"r1c0", "r1c1", "r1c2", "r1c3"},
				{"r2c0r2c1r2c2"},
				{"r3c0r3c1r3c2"},
				{"r4c0", "r4c1", "r4c2"},
			},
			action: &ProcessorMergeColumns{
				Name:  "Test 1",
				Start: Cell{Row: 2, Column: 0},
				End:   Cell{Row: 4, Column: -1},
			},
		},
		{
			input: testCopyInput(input),
			output: [][]string{
				{"r0c0", "r0c1", "r0c2"},
				{"r1c0", "r1c1", "r1c2", "r1c3"},
				{"r2c0r2c1r2c2"},
				{"r3c0r3c1r3c2"},
				{"r4c0r4c1r4c2"},
			},
			action: &ProcessorMergeColumns{
				Name:  "Test 2",
				Start: Cell{Row: 2, Column: 0},
				End:   Cell{Row: -1, Column: -1},
			},
		},
		{
			input: testCopyInput(input),
			output: [][]string{
				{"r0c0", "r0c1", "r0c2"},
				{"r1c0", "r1c1", "r1c2", "r1c3"},
				{"r2c0,r2c1,r2c2"},
				{"r3c0,r3c1,r3c2"},
				{"r4c0,r4c1,r4c2"},
			},
			action: &ProcessorMergeColumns{
				Name:      "Test 3",
				Start:     Cell{Row: 2, Column: 0},
				End:       Cell{Row: -1, Column: -1},
				Delimiter: ",",
			},
		},
		{
			input: testCopyInput(input),
			output: [][]string{
				{"r0c0r0c1r0c2"},
				{"r1c0r1c1r1c2r1c3"},
				{"r2c0r2c1r2c2"},
				{"r3c0r3c1r3c2"},
				{"r4c0r4c1r4c2"},
			},
			action: &ProcessorMergeColumns{
				Name:  "Test 4",
				Start: Cell{Row: 0, Column: 0},
				End:   Cell{Row: -1, Column: -1},
			},
		},
		{
			input: testCopyInput(input),
			action: &ProcessorMergeColumns{
				Name:  "Test 5",
				Start: Cell{Row: -1, Column: 0},
				End:   Cell{Row: -1, Column: -1},
			},
			expectFail: true,
		},
		{
			input: testCopyInput(input),
			action: &ProcessorMergeColumns{
				Name:  "Test 6",
				Start: Cell{Row: 0, Column: -1},
				End:   Cell{Row: -1, Column: -1},
			},
			expectFail: true,
		},
		{
			input: testCopyInput(input),
			action: &ProcessorMergeColumns{
				Name:  "Test 7",
				Start: Cell{Row: -1, Column: -1},
				End:   Cell{Row: -1, Column: -1},
			},
			expectFail: true,
		},
		{
			input: testCopyInput(input),
			action: &ProcessorMergeColumns{
				Name:  "Test 8",
				Start: Cell{Row: 0, Column: 0},
				End:   Cell{Row: 10, Column: -1},
			},
			expectFail: true,
		},
		{
			input: testCopyInput(input),
			action: &ProcessorMergeColumns{
				Name:  "Test 9",
				Start: Cell{Row: 0, Column: 0},
				End:   Cell{Row: -1, Column: 10},
			},
			expectFail: true,
		},
		{
			input: testCopyInput(input),
			action: &ProcessorMergeColumns{
				Name:  "Test 10",
				Start: Cell{Row: 0, Column: 0},
				End:   Cell{Row: -1, Column: 10},
			},
			expectFail: true,
		},
		{
			input: testCopyInput(input),
			action: &ProcessorMergeColumns{
				Name:  "Test 11",
				Start: Cell{Row: 1, Column: 0},
				End:   Cell{Row: 0, Column: -1},
			},
			expectFail: true,
		},
		{
			input: testCopyInput(input),
			action: &ProcessorMergeColumns{
				Name:  "Test 12",
				Start: Cell{Row: 1, Column: 1},
				End:   Cell{Row: -1, Column: 0},
			},
			expectFail: true,
		},
		{
			input: testCopyInput(input),
			output: [][]string{
				{"r0c0", "r0c1", "r0c2"},
				{"r1c0", "r1c1", "r1c2", "r1c3"},
				{"r2c0", "r2c1r2c2"},
				{"r3c0", "r3c1r3c2"},
				{"r4c0", "r4c1", "r4c2"},
			},
			action: &ProcessorMergeColumns{
				Name:  "Test 13",
				Start: Cell{Row: 2, Column: 1},
				End:   Cell{Row: 4, Column: -1},
			},
		},
		{
			input: [][]string{
				{"r0c0", "r0c1", "r0c2"},
				{"r1c0", "r1c1", "r1c2", "r1c3"},
				{"r2c0", "r2c1", "r2c2", "r2c3"},
				{"r3c0", "r3c1", "r3c2", "r3c3"},
				{"r4c0", "r4c1", "r4c2", "r4c3"},
			},
			output: [][]string{
				{"r0c0", "r0c1", "r0c2"},
				{"r1c0", "r1c1", "r1c2", "r1c3"},
				{"r2c0", "r2c1r2c2", "r2c3"},
				{"r3c0", "r3c1r3c2", "r3c3"},
				{"r4c0", "r4c1", "r4c2", "r4c3"},
			},
			action: &ProcessorMergeColumns{
				Name:  "Test 14",
				Start: Cell{Row: 2, Column: 1},
				End:   Cell{Row: 4, Column: 3},
			},
		},
	}

	for n, test := range tests {
		output, err := test.action.Execute(test.input)
		if err != nil {
			if test.expectFail {
				continue
			}
			t.Errorf("Test %d: error executing: %v", n, err)
		} else {
			if !reflect.DeepEqual(test.output, output) {
				t.Errorf("Test %d: output does not match expectation\nexpected: %v\nreceived: %v", n, test.output, output)
			}
		}
	}
}

func TestProcessorMergeRows(t *testing.T) {
	input := [][]string{
		{"r0c0", "r0c1", "r0c2"},
		{"r1c0", "r1c1", "r1c2", "r1c3"},
		{"r2c0", "r2c1", "r2c2"},
		{"r3c0", "r3c1", "r3c2"},
		{"r4c0", "r4c1", "r4c2"},
		{"r5c0", "r5c1", "r5c2"},
	}
	tests := []testPreProcessor{
		{
			input: testCopyInput(input),
			output: [][]string{
				{"r0c0", "r0c1", "r0c2"},
				{"r1c0", "r1c1", "r1c2", "r1c3"},
				{"r2c0r3c0", "r2c1r3c1", "r2c2r3c2"},
				{"r4c0", "r4c1", "r4c2"},
				{"r5c0", "r5c1", "r5c2"},
			},
			action: &ProcessorMergeRows{
				Name:     "Test 0",
				StartRow: 2,
				EndRow:   4,
			},
		},
		{
			input: testCopyInput(input),
			output: [][]string{
				{"r0c0", "r0c1", "r0c2"},
				{"r1c0", "r1c1", "r1c2", "r1c3"},
				{"r2c0r3c0r4c0r5c0", "r2c1r3c1r4c1r5c1", "r2c2r3c2r4c2r5c2"},
			},
			action: &ProcessorMergeRows{
				Name:     "Test 1",
				StartRow: 2,
				EndRow:   -1,
			},
		},
		{
			input: testCopyInput(input),
			output: [][]string{
				{"r0c0", "r0c1", "r0c2"},
				{"r1c0", "r1c1", "r1c2", "r1c3"},
				{"r2c0,r3c0,r4c0,r5c0", "r2c1,r3c1,r4c1,r5c1", "r2c2,r3c2,r4c2,r5c2"},
			},
			action: &ProcessorMergeRows{
				Name:      "Test 2",
				StartRow:  2,
				EndRow:    -1,
				Delimiter: ",",
			},
		},
		{
			input: testCopyInput(input),
			action: &ProcessorMergeRows{
				Name:      "Test 3",
				StartRow:  0,
				EndRow:    -1,
				Delimiter: ",",
			},
			expectFail: true,
		},
		{
			input: testCopyInput(input),
			action: &ProcessorMergeRows{
				Name:     "Test 4",
				StartRow: -1,
				EndRow:   -1,
			},
			expectFail: true,
		},
		{
			input: testCopyInput(input),
			action: &ProcessorMergeRows{
				Name:     "Test 5",
				StartRow: 0,
				EndRow:   10,
			},
			expectFail: true,
		},
		{
			input: testCopyInput(input),
			action: &ProcessorMergeRows{
				Name:     "Test 6",
				StartRow: 1,
				EndRow:   0,
			},
			expectFail: true,
		},
		{
			input: [][]string{
				{"r0c0", "r0c1", "r0c2"},
				{"r1c0", "r1c1", "r1c2", "r1c3"},
				{"r2c0", "r2c1 ", " "},
				{"r3c0", "r3c1 ", "r3c2"},
				{"r4c0", "r4c1 ", "r4c2"},
				{"r5c0", "r5c1", "r5c2"},
			},
			output: [][]string{
				{"r0c0", "r0c1", "r0c2"},
				{"r1c0", "r1c1", "r1c2", "r1c3"},
				{"r2c0,r3c0,r4c0,r5c0", "r2c1,r3c1,r4c1,r5c1", ",r3c2,r4c2,r5c2"},
			},
			action: &ProcessorMergeRows{
				Name:           "Test 7",
				StartRow:       2,
				EndRow:         -1,
				Delimiter:      ",",
				TrimWhitespace: true,
			},
		},
	}

	for n, test := range tests {
		output, err := test.action.Execute(test.input)
		if err != nil {
			if test.expectFail {
				continue
			}
			t.Errorf("Test %d: error executing: %v", n, err)
		} else {
			if !reflect.DeepEqual(test.output, output) {
				t.Errorf("Test %d: output does not match expectation\nexpected: %v\nreceived: %v", n, test.output, output)
			}
		}
	}
}

func TestProcessorFillRight(t *testing.T) {
	input := [][]string{
		{"r0c0", "r0c1", "r0c2"},
		{"r1c0", "r1c1", "", "r1c3", "r1c4"},
		{"r2c0", "", "r2c2", ""},
		{"r3c0", "r3c1", "r3c2", "", "r3c4"},
		{"r4c0", "", "r4c2", ""},
		{"", "", "r5c2", "r5c3"},
	}
	tests := []testPreProcessor{
		{
			input: testCopyInput(input),
			output: [][]string{
				{"r0c0", "r0c1", "r0c2"},
				{"r1c0", "r1c1", "", "r1c3", "r1c4"},
				{"r2c0", "r2c0", "r2c2", ""},
				{"r3c0", "r3c1", "r3c2", "", "r3c4"},
				{"r4c0", "", "r4c2", ""},
				{"", "", "r5c2", "r5c3"},
			},
			action: &ProcessorFillRight{
				Name:  "Test 0",
				Start: Cell{Row: 2, Column: 0},
				End:   Cell{Row: 4, Column: 2},
			},
		},
		{
			input: testCopyInput(input),
			output: [][]string{
				{"r0c0", "r0c1", "r0c2"},
				{"r1c0", "r1c1", "", "r1c3", "r1c4"},
				{"r2c0", "r2c0", "r2c2", "r2c2"},
				{"r3c0", "r3c1", "r3c2", "r3c2", "r3c4"},
				{"r4c0", "", "r4c2", ""},
				{"", "", "r5c2", "r5c3"},
			},
			action: &ProcessorFillRight{
				Name:  "Test 1",
				Start: Cell{Row: 2, Column: 0},
				End:   Cell{Row: 4, Column: -1},
			},
		},
		{
			input: testCopyInput(input),
			output: [][]string{
				{"r0c0", "r0c1", "r0c2"},
				{"r1c0", "r1c1", "", "r1c3", "r1c4"},
				{"r2c0", "r2c0", "r2c2", "r2c2"},
				{"r3c0", "r3c1", "r3c2", "r3c2", "r3c4"},
				{"r4c0", "r4c0", "r4c2", "r4c2"},
				{"", "", "r5c2", "r5c3"},
			},
			action: &ProcessorFillRight{
				Name:  "Test 2",
				Start: Cell{Row: 2, Column: 0},
				End:   Cell{Row: -1, Column: -1},
			},
		},
		{
			input: testCopyInput(input),
			output: [][]string{
				{"r0c0", "r0c1", "r0c2"},
				{"r1c0", "r1c1", "r1c1", "r1c3", "r1c4"},
				{"r2c0", "r2c0", "r2c2", "r2c2"},
				{"r3c0", "r3c1", "r3c2", "r3c2", "r3c4"},
				{"r4c0", "r4c0", "r4c2", "r4c2"},
				{"", "", "r5c2", "r5c3"},
			},
			action: &ProcessorFillRight{
				Name:  "Test 3",
				Start: Cell{Row: 0, Column: 0},
				End:   Cell{Row: -1, Column: -1},
			},
		},
		{
			input: testCopyInput(input),
			action: &ProcessorFillRight{
				Name:  "Test 4",
				Start: Cell{Row: -1, Column: 0},
				End:   Cell{Row: -1, Column: -1},
			},
			expectFail: true,
		},
		{
			input: testCopyInput(input),
			action: &ProcessorFillRight{
				Name:  "Test 5",
				Start: Cell{Row: 0, Column: -1},
				End:   Cell{Row: -1, Column: -1},
			},
			expectFail: true,
		},
		{
			input: testCopyInput(input),
			action: &ProcessorFillRight{
				Name:  "Test 6",
				Start: Cell{Row: -1, Column: -1},
				End:   Cell{Row: -1, Column: -1},
			},
			expectFail: true,
		},
		{
			input: testCopyInput(input),
			action: &ProcessorFillRight{
				Name:  "Test 7",
				Start: Cell{Row: 0, Column: 0},
				End:   Cell{Row: 10, Column: -1},
			},
			expectFail: true,
		},
		{
			input: testCopyInput(input),
			action: &ProcessorFillRight{
				Name:  "Test 8",
				Start: Cell{Row: 0, Column: 0},
				End:   Cell{Row: -1, Column: 10},
			},
			expectFail: true,
		},
		{
			input: testCopyInput(input),
			action: &ProcessorFillRight{
				Name:  "Test 9",
				Start: Cell{Row: 0, Column: 0},
				End:   Cell{Row: -1, Column: 10},
			},
			expectFail: true,
		},
		{
			input: testCopyInput(input),
			action: &ProcessorFillRight{
				Name:  "Test 10",
				Start: Cell{Row: 1, Column: 0},
				End:   Cell{Row: 0, Column: -1},
			},
			expectFail: true,
		},
		{
			input: testCopyInput(input),
			action: &ProcessorFillRight{
				Name:  "Test 11",
				Start: Cell{Row: 1, Column: 1},
				End:   Cell{Row: -1, Column: 0},
			},
			expectFail: true,
		},
		{
			input: testCopyInput(input),
			output: [][]string{
				{"r0c0", "r0c1", "r0c2"},
				{"r1c0", "r1c1", "", "r1c3", "r1c4"},
				{"r2c0", "", "r2c2", "r2c2"},
				{"r3c0", "r3c1", "r3c2", "r3c2", "r3c4"},
				{"r4c0", "", "r4c2", ""},
				{"", "", "r5c2", "r5c3"},
			},
			action: &ProcessorFillRight{
				Name:  "Test 12",
				Start: Cell{Row: 2, Column: 2},
				End:   Cell{Row: 4, Column: -1},
			},
		},
	}

	for n, test := range tests {
		output, err := test.action.Execute(test.input)
		if err != nil {
			if test.expectFail {
				continue
			}
			t.Errorf("Test %d: error executing: %v", n, err)
		} else {
			if !reflect.DeepEqual(test.output, output) {
				t.Errorf("Test %d: output does not match expectation\nexpected: %v\nreceived: %v", n, test.output, output)
			}
		}
	}
}

func TestProcessorReplaceCell(t *testing.T) {
	input := [][]string{
		{"r0c0", "r0c1", "r0c2"},
		{"r1c0", "r1c1", "r1c2"},
		{"r2c0", "r2c1", "r2c2"},
		{"r3c0", "r3c1", "r3c2"},
	}
	tests := []testPreProcessor{
		{
			input: testCopyInput(input),
			output: [][]string{
				{"header0", "r0c1", "r0c2"},
				{"r1c0", "r1c1", "r1c2"},
				{"r2c0", "r2c1", "r2c2"},
				{"r3c0", "r3c1", "r3c2"},
			},
			action: &ProcessorReplaceCell{
				Name:  "Test 0",
				Cell:  Cell{Row: 0, Column: 0},
				Value: "header0",
			},
		},
		{
			input: testCopyInput(input),
			action: &ProcessorReplaceCell{
				Name: "Test 1",
				Cell: Cell{Row: -1, Column: 0},
			},
			expectFail: true,
		},
		{
			input: testCopyInput(input),
			action: &ProcessorReplaceCell{
				Name: "Test 2",
				Cell: Cell{Row: -1, Column: -1},
			},
			expectFail: true,
		},
		{
			input: testCopyInput(input),
			action: &ProcessorReplaceCell{
				Name: "Test 3",
				Cell: Cell{Row: 0, Column: -1},
			},
			expectFail: true,
		},
		{
			input: testCopyInput(input),
			action: &ProcessorReplaceCell{
				Name: "Test 4",
				Cell: Cell{Row: 10, Column: 0},
			},
			expectFail: true,
		},
		{
			input: testCopyInput(input),
			action: &ProcessorReplaceCell{
				Name: "Test 5",
				Cell: Cell{Row: 0, Column: 10},
			},
			expectFail: true,
		},
	}

	for n, test := range tests {
		output, err := test.action.Execute(test.input)
		if err != nil {
			if test.expectFail {
				continue
			}
			t.Errorf("Test %d: error executing: %v", n, err)
		} else {
			if !reflect.DeepEqual(test.output, output) {
				t.Errorf("Test %d: output does not match expectation\nexpected: %v\nreceived: %v", n, test.output, output)
			}
		}
	}
}

func TestProcessorTransposeRow(t *testing.T) {
	input := [][]string{
		{"r0c0", "r0c1", "r0c2"},
		{"r1c0", "r1c1", "r1c2", "r1c3"},
		{"r2c0", "r2c1", "r2c2"},
		{"r3c0", "r3c1", "r3c2"},
		{"r4c0", "r4c1", "r4c2"},
		{"r5c0", "r5c1", "r5c2"},
	}
	tests := []testPreProcessor{
		{
			input: testCopyInput(input),
			output: [][]string{
				{"r0c0", "r0c1", "r0c2"},
				{"r1c0", "r1c1", "r1c2", "r1c3"},
				{"r2c0", "r3c0"},
				{"r2c1", "r3c1"},
				{"r2c2", "r3c2"},
				{"r4c0", "r4c1", "r4c2"},
				{"r5c0", "r5c1", "r5c2"},
			},
			action: &ProcessorTransposeRow{
				Name:     "Test 0",
				StartRow: 2,
				EndRow:   4,
			},
		},
		{
			input: testCopyInput(input),
			output: [][]string{
				{"r0c0", "r0c1", "r0c2"},
				{"r1c0", "r1c1", "r1c2", "r1c3"},
				{"r2c0", "r3c0", "r4c0", "r5c0"},
				{"r2c1", "r3c1", "r4c1", "r5c1"},
				{"r2c2", "r3c2", "r4c2", "r5c2"},
			},
			action: &ProcessorTransposeRow{
				Name:     "Test 1",
				StartRow: 2,
				EndRow:   -1,
			},
		},
		{
			input: testCopyInput(input),
			action: &ProcessorTransposeRow{
				Name:     "Test 2",
				StartRow: 0,
				EndRow:   -1,
			},
			expectFail: true,
		},
		{
			input: testCopyInput(input),
			action: &ProcessorTransposeRow{
				Name:     "Test 3",
				StartRow: -1,
				EndRow:   -1,
			},
			expectFail: true,
		},
		{
			input: testCopyInput(input),
			action: &ProcessorTransposeRow{
				Name:     "Test 4",
				StartRow: 0,
				EndRow:   10,
			},
			expectFail: true,
		},
		{
			input: testCopyInput(input),
			action: &ProcessorTransposeRow{
				Name:     "Test 5",
				StartRow: 1,
				EndRow:   0,
			},
			expectFail: true,
		},
	}

	for n, test := range tests {
		output, err := test.action.Execute(test.input)
		if err != nil {
			if test.expectFail {
				continue
			}
			t.Errorf("Test %d: error executing: %v", n, err)
		} else {
			if !reflect.DeepEqual(test.output, output) {
				t.Errorf("Test %d: output does not match expectation\nexpected: %v\nreceived: %v", n, test.output, output)
			}
		}
	}
}

func TestProcessorRemoveCellLeft(t *testing.T) {
	input := [][]string{
		{"r0c0", "r0c1", "r0c2"},
		{"r1c0", "r1c1", "r1c2"},
		{"r2c0", "r2c1", "r2c2"},
		{"r3c0", "r3c1", "r3c2"},
	}
	tests := []testPreProcessor{
		{
			input: testCopyInput(input),
			output: [][]string{
				{"r0c1", "r0c2"},
				{"r1c0", "r1c1", "r1c2"},
				{"r2c0", "r2c1", "r2c2"},
				{"r3c0", "r3c1", "r3c2"},
			},
			action: &ProcessorRemoveCellLeft{
				Name: "Test 0",
				Cell: Cell{Row: 0, Column: 0},
			},
		},
		{
			input: testCopyInput(input),
			action: &ProcessorRemoveCellLeft{
				Name: "Test 1",
				Cell: Cell{Row: -1, Column: 0},
			},
			expectFail: true,
		},
		{
			input: testCopyInput(input),
			action: &ProcessorRemoveCellLeft{
				Name: "Test 2",
				Cell: Cell{Row: -1, Column: -1},
			},
			expectFail: true,
		},
		{
			input: testCopyInput(input),
			action: &ProcessorRemoveCellLeft{
				Name: "Test 3",
				Cell: Cell{Row: 0, Column: -1},
			},
			expectFail: true,
		},
		{
			input: testCopyInput(input),
			action: &ProcessorRemoveCellLeft{
				Name: "Test 4",
				Cell: Cell{Row: 10, Column: 0},
			},
			expectFail: true,
		},
		{
			input: testCopyInput(input),
			action: &ProcessorRemoveCellLeft{
				Name: "Test 5",
				Cell: Cell{Row: 0, Column: 10},
			},
			expectFail: true,
		},
		{
			input: testCopyInput(input),
			output: [][]string{
				{"r0c0", "r0c1"},
				{"r1c0", "r1c1", "r1c2"},
				{"r2c0", "r2c1", "r2c2"},
				{"r3c0", "r3c1", "r3c2"},
			},
			action: &ProcessorRemoveCellLeft{
				Name: "Test 6",
				Cell: Cell{Row: 0, Column: 2},
			},
		},
		{
			input: testCopyInput(input),
			output: [][]string{
				{"r0c0", "r0c1", "r0c2"},
				{"r1c0", "r1c1", "r1c2"},
				{"r2c0", "r2c1", "r2c2"},
				{"r3c0", "r3c1"},
			},
			action: &ProcessorRemoveCellLeft{
				Name: "Test 7",
				Cell: Cell{Row: 3, Column: 2},
			},
		},
	}

	for n, test := range tests {
		output, err := test.action.Execute(test.input)
		if err != nil {
			if test.expectFail {
				continue
			}
			t.Errorf("Test %d: error executing: %v", n, err)
		} else {
			if !reflect.DeepEqual(test.output, output) {
				t.Errorf("Test %d: output does not match expectation\nexpected: %v\nreceived: %v", n, test.output, output)
			}
		}
	}
}
