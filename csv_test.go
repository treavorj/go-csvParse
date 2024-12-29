package csvParse

import (
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"
)

var testRecords = [][]string{
	{"Row0Column0", "Row0Column1"},
	{"Row1Column0", "Row1Column1"},
	{"timestamp", "240910T12:53:09"},
	{"Int", "Float", "String", "Bool", "YYdMMdDD_HHcMMcSS"},
	{"1", "1", "Hello World", "true", "2024-09-10 12:53:09"},
	{"2", "2", "Goodbye World", "false", "2024-09-11 17:09:06"},
}

func TestMarshaling(t *testing.T) {
	t.Parallel()

	config := Csv{
		PreProcessor: []Processor{
			&ProcessorRemoveCellLeft{
				Name: "TestRemoveCellLeft",
				Cell: Cell{Row: 5, Column: 5},
			},
			&ProcessorFillRight{
				Name:  "TestFillRight",
				Start: Cell{Row: 0, Column: 0},
				End:   Cell{Row: -1, Column: -1},
			},
			&ProcessorMergeRows{
				Name:           "TestMergeRows",
				StartRow:       0,
				EndRow:         2,
				Delimiter:      " ",
				TrimWhitespace: true,
			},
		},
		CellLocations: []CellLocation{
			{
				NameCell: Cell{Row: 0, Column: 0},
				Location: Cell{Row: 0, Column: 1},
				DataType: DataTypeFloat64,
			},
		},
		ConcatCellLocations: []ConcatCellLocation{
			{
				Name: "@timestamp",
				Cells: []Cell{
					{Row: 1, Column: 1},
					{Row: 2, Column: 1},
				},
				Delimiter: " ",
				DataType:  DataTypeDateTimeStyle1,
			},
		},
		TableLocations: []TableLocation{
			{
				Name:           "metrics",
				StartCell:      Cell{Row: 5, Column: 0},
				EndCell:        Cell{Row: 9, Column: 3},
				TableHasHeader: true,
				ColumnDataTypes: []DataType{
					DataTypeInt64, DataTypeString, DataTypeString, DataTypeFloat64,
				},
			},
		},
	}

	configJson, err := json.Marshal(config)
	if err != nil {
		t.Fatalf("error marshalling config: %v", err)
	}
	t.Logf("configJson: %s", configJson)

	var configFromJson Csv
	err = json.Unmarshal(configJson, &configFromJson)
	if err != nil {
		t.Fatalf("error unmarshaling config: %v", err)
	} else if !reflect.DeepEqual(config, configFromJson) {
		t.Errorf("config not equal to configFromJson\nexpected: %v\nreceived: %v", config, configFromJson)
	}
}

func TestCellValues(t *testing.T) {
	t.Parallel()
	cellLocations := []CellLocation{
		{Name: "Row0Column0", Location: Cell{Row: 0, Column: 0}, DataType: DataTypeString},
		{Name: "Row0Column1", Location: Cell{Row: 0, Column: 1}, DataType: DataTypeString},
		{Name: "Row1Column0", Location: Cell{Row: 1, Column: 0}, DataType: DataTypeString},
		{Name: "Row1Column1", Location: Cell{Row: 1, Column: 1}, DataType: DataTypeString},
		{Name: "Int", Location: Cell{Row: 4, Column: 0}, DataType: DataTypeInt64, NameCell: Cell{Row: 3, Column: 0}},
		{Name: "Float", Location: Cell{Row: 4, Column: 1}, DataType: DataTypeFloat64, NameCell: Cell{Row: 3, Column: 1}},
		{Name: "String", Location: Cell{Row: 4, Column: 2}, DataType: DataTypeString, NameCell: Cell{Row: 3, Column: 2}},
		{Name: "Bool", Location: Cell{Row: 4, Column: 3}, DataType: DataTypeBool, NameCell: Cell{Row: 3, Column: 3}},
		{Name: "YYdMMdDD_HHcMMcSS", Location: Cell{Row: 4, Column: 4}, DataType: DataTypeDateTimeStyle0, NameCell: Cell{Row: 3, Column: 4}},
	}
	values := []any{"Row0Column0", "Row0Column1", "Row1Column0", "Row1Column1", int64(1), Float64(1), "Hello World", true, time.Date(2024, 9, 10, 12, 53, 9, 0, time.Local).Format(time.RFC3339)}
	targetTime := time.Date(2024, 9, 10, 12, 53, 9, 0, time.Local).Format(time.RFC3339)
	finalAnswer := map[string]any{
		"Int":               float64(1),
		"Float":             float64(1),
		"String":            "Hello World",
		"Bool":              true,
		"YYdMMdDD_HHcMMcSS": "2024-09-10T12:53:09-" + targetTime[len(targetTime)-5:],
		"@timestamp":        targetTime,
	}

	for n, location := range cellLocations {
		t.Logf("%d: %v", n, location)
		name, value, err := location.Parse(testRecords)
		if err != nil {
			t.Errorf("Error with %s:%v", location.Name, err)
		}
		if name != location.Name {
			t.Errorf("Found (%s) for name instead of (%s)", name, location.Name)
		}
		if value != values[n] {
			t.Errorf("Found Value (%v of type %T) instead of (%v of type %T)", value, value, values[n], values[n])
		}
	}

	csv := Csv{CellLocations: cellLocations[4:], TimeField: TimeField{Cells: []Cell{{Row: 2, Column: 1}}, Layout: "060102T15:04:05"}}
	data, err := csv.ParseRecords(testRecords)
	if err != nil {
		t.Fatalf("error parsing records: %v", err)
	}

	dataJson, err := json.Marshal(data)
	if err != nil {
		t.Fatalf("failed to marshall data: %v", err)
	}

	var dataJsonParse map[string]any
	err = json.Unmarshal(dataJson, &dataJsonParse)
	if err != nil {
		t.Fatalf("failed to unMarshall data: %v", err)
	}

	for key, value := range dataJsonParse {
		finalValue, exists := finalAnswer[key]
		if !exists {
			t.Errorf("key (%s) not found", key)
			continue
		}
		if finalValue != value {
			t.Errorf("%s: value (%v type %T) does not equal finalValue (%v type %T)", key, value, value, finalValue, finalValue)
		}
	}
}

func TestTableValuesAsArrayFlexible(t *testing.T) {
	t.Parallel()
	tableLocation := TableLocation{
		Name:            "data",
		StartCell:       Cell{Row: 3, Column: 0},
		EndCell:         Cell{Row: -1, Column: -1},
		TableHasHeader:  true,
		ColumnDataTypes: []DataType{DataTypeInt64, DataTypeFloat64, DataTypeString, DataTypeBool, DataTypeDateTimeStyle0},
		ParseAsArray:    true,
	}
	csv := Csv{TableLocations: []TableLocation{tableLocation}}
	data, err := csv.ParseRecords(testRecords)
	if err != nil {
		t.Fatalf("failed to parse data: %v", err)
	}
	dataTyped := data.(map[string]any)
	if len(dataTyped) != 1 {
		t.Errorf("invalid length of tables. Found: %d", len(dataTyped))
	}
	dataTable := dataTyped["data"].(map[string][]any)
	if len(dataTable) != 5 {
		t.Errorf("invalid length of objects. Found: %d", len(dataTyped))
	}
	if len(dataTable["Int"]) != 2 {
		t.Errorf("invalid length of rows. Found: %d", len(dataTable["Int"]))
	}
}
func TestTableValuesAsArrayFixed(t *testing.T) {
	t.Parallel()
	tableLocation := TableLocation{
		Name:            "data",
		StartCell:       Cell{Row: 3, Column: 0},
		EndCell:         Cell{Row: 5, Column: 4},
		TableHasHeader:  true,
		ColumnDataTypes: []DataType{DataTypeInt64, DataTypeFloat64, DataTypeString, DataTypeBool, DataTypeDateTimeStyle0},
		ParseAsArray:    true,
	}
	csv := Csv{TableLocations: []TableLocation{tableLocation}}
	data, err := csv.ParseRecords(testRecords)
	if err != nil {
		t.Fatalf("failed to parse data: %v", err)
	}
	dataTyped := data.(map[string]any)
	if len(dataTyped) != 1 {
		t.Errorf("invalid length of tables. Found: %d", len(dataTyped))
	}
	dataTable := dataTyped["data"].(map[string][]any)
	if len(dataTable) != 5 {
		t.Errorf("invalid length of objects. Found: %d", len(dataTyped))
	}
	if len(dataTable["Int"]) != 2 {
		t.Errorf("invalid length of rows. Found: %d", len(dataTable["Int"]))
	}
}

func TestCsvStyle1(t *testing.T) {
	t.Parallel()

	input := `
Date,Time,Shot,Drop,Temperature,Alarm1,Alarm2,Alarm3,Alarm4,Alarm5,Alarm6,Alarm7,Alarm8,Alarm9,Alarm10,Alarm11,Alarm12,Alarm13,Alarm14,Alarm15,Alarm16,Alarm17,Alarm18,Alarm19,Alarm20,Alarm21,Alarm22,Alarm23,Alarm24,Alarm25,Alarm26,Alarm27,Alarm28,Alarm29,Alarm30,Alarm31,Alarm32,Alarm33,Alarm34,Alarm35,Alarm36,Alarm37,Alarm38,Alarm39,Alarm40,Alarm41,Alarm42,Alarm43,Alarm44,Alarm45,Alarm46,Alarm47,Alarm48,Cycle Time          ,D-LckForce Hlp [Up] ,Low-Velocity Speed  ,High-Velocity Speed ,Press.-Up Time      ,Max. Casting Press. ,Biscuit size        ,H-Spd. StrokePostion,Section of Hi-Speed ,Fill-Completion Pos.,Peak-Velocity Speed ,Weight of The Shot  ,Lo-Vel. Acceleration,Hi-Vel. Acceleration,L-Spd.RateOfVariable,H-Spd.RateOfVariable,Filling Press.      ,Filling Time        ,Decelerating Stroke ,Eject Forword Limit ,Decelerating section,Die Height Postion  ,PourRetMeasuringAngl,Decelerated Velocity,VcuumValve Shut Pos.,Move Die,Fix. Die,Intensive Time      ,M.Die Temp Sig.,F.Die Temp Sig.,Filling StartingPos.,Press.UpStartingPos.,InjctStrt StrtVelACC,InjctEnd StartVelACC,InjctStrt Press. ACC,InjctEnd Press. ACC ,Max Shut Press.     ,Spray,Core In,DieClose,Pouring,Shot,WorkCool,Die Open,Core Out,Eject,Take-Out,Pres Up Valve Rev,WaveFile,Spray,,,,,Core In,,,,,DieClose,,,,,Pouring,,,,,Shot,,,,,WorkCool,,,,,Die Open,,,,,Core Out,,,,,Eject,,,,,Take-Out,,,,,Item11,,,,,Item12,,,,,Item13,,,,,Item14,,,,,Total Cycle
YYYY/MM/DD, hh:mm:ss,No.,,Start-up pattern,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,sec,%,m/s,m/s,msec,MPa,mm,mm,mm,mm,m/s,kg,msec,msec,%,%,MPa,msec,mm,mm,mm,mm,deg,m/s,mm,L/m,L/m,msec,℃,℃,mm,mm,MPa,MPa,MPa,MPa,MPa,sec,sec,sec,sec,sec,sec,sec,sec,sec,sec,rev,,1st,1ed,2st,2ed,Cycle,1st,1ed,2st,2ed,Cycle,1st,1ed,2st,2ed,Cycle,1st,1ed,2st,2ed,Cycle,1st,1ed,2st,2ed,Cycle,1st,1ed,2st,2ed,Cycle,1st,1ed,2st,2ed,Cycle,1st,1ed,2st,2ed,Cycle,1st,1ed,2st,2ed,Cycle,1st,1ed,2st,2ed,Cycle,1st,1ed,2st,2ed,Cycle,1st,1ed,2st,2ed,Cycle,1st,1ed,2st,2ed,Cycle,1st,1ed,2st,2ed,Cycle,
2024/09/23,08:04:18,989301,0,0,0,0,0,0,0,0,2,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,1,0,0,0,0,0,0,0,0,0,0,0,55.3,93,0.148,-0.001,-1,21.9,36.3,-0.1,-0.1,489.3,0.328,4.80,12,-1,19.2,-0.1,-0.1,-1,-0.1,34.5,-0.1,111.95,45.3,-0.001,94.4,93.4,9.5,-1,109,101,185.3,-0.1,11.03,10.48,10.66,10.57,10.40,0.0,2.8,2.0,9.2,7.0,8.4,1.7,2.6,0.3,10.7,1.332,240923\989301A.WBA,0.0,0.0,0.0,0.0,0.0,2.3,4.9,0.0,0.0,2.6,5.1,7.1,0.0,0.0,2.0,25.3,34.5,0.0,0.0,9.2,35.0,42.0,0.0,0.0,7.0,40.2,48.6,0.0,0.0,8.4,48.6,50.3,0.0,0.0,1.7,50.5,53.1,0.0,0.0,2.6,53.5,53.8,0.0,0.0,0.3,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,55.3
2024/09/23,08:05:14,989302,0,0,0,0,0,0,0,0,2,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,1,0,0,0,0,0,0,0,0,0,0,0,56.2,93,0.148,3.408,-1,21.6,35.3,187.6,302.7,490.3,3.664,4.65,6,26,-0.2,8.0,-0.1,-1,485.8,33.9,4.5,111.95,45.3,0.000,92.0,93.6,9.4,-1,109,101,185.4,-0.1,11.13,10.39,10.60,10.57,10.27,0.0,8.9,2.0,9.2,5.3,8.4,1.7,10.7,0.3,10.7,1.332,240923\989302A.WBA,0.0,0.0,0.0,0.0,0.0,20.1,22.6,0.0,0.0,2.5,22.8,24.8,0.0,0.0,2.0,26.2,35.4,0.0,0.0,9.2,35.9,41.2,0.0,0.0,5.3,41.1,49.5,0.0,0.0,8.4,49.5,51.2,0.0,0.0,1.7,51.4,54.0,0.0,0.0,2.6,54.4,54.7,0.0,0.0,0.3,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,56.2
2024/09/23,08:06:10,989303,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,1,0,0,0,0,0,0,0,0,0,0,0,56.5,92,0.150,3.413,-1,21.5,24.2,188.3,313.1,501.4,3.656,4.65,4,24,-0.2,7.0,-0.1,-1,498.6,34.2,2.8,111.97,44.1,0.000,92.2,93.4,9.4,-1,109,101,186.6,-0.1,10.91,10.36,10.65,10.66,10.20,0.0,15.1,2.0,9.2,5.3,8.5,1.7,16.6,0.3,10.7,1.332,240923\989303A.WBA,0.0,0.0,0.0,0.0,0.0,19.0,21.5,0.0,0.0,2.5,21.7,23.7,0.0,0.0,2.0,26.5,35.7,0.0,0.0,9.2,36.2,41.5,0.0,0.0,5.3,41.3,49.8,0.0,0.0,8.5,49.8,51.5,0.0,0.0,1.7,51.7,54.3,0.0,0.0,2.6,54.7,55.0,0.0,0.0,0.3,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,56.5
2024/09/23,08:07:06,989304,1,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,56.5,93,0.149,3.397,46,53.6,17.5,187.9,320.2,508.1,3.648,4.65,8,26,-0.2,8.5,1.1,474,504.8,33.8,3.3,111.98,44.1,0.025,92.8,93.2,9.4,46,109,101,186.6,32.8,10.89,10.30,10.56,10.16,25.52,0.0,18.2,2.0,9.2,5.3,8.5,1.7,20.0,0.3,10.9,1.332,240923\989304A.WBA,0.0,0.0,0.0,0.0,0.0,15.7,18.2,0.0,0.0,2.5,18.4,20.4,0.0,0.0,2.0,26.4,35.6,0.0,0.0,9.2,36.1,41.4,0.0,0.0,5.3,41.2,49.7,0.0,0.0,8.5,49.7,51.4,0.0,0.0,1.7,51.6,54.2,0.0,0.0,2.6,54.6,54.9,0.0,0.0,0.3,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,56.5
2024/09/23,08:08:08,989305,1,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,60.1,93,0.148,3.394,30,54.6,17.2,187.8,320.6,508.4,3.625,4.65,6,24,-0.2,8.0,5.7,40,505.3,33.5,3.1,111.98,44.1,0.019,92.8,92.8,9.5,30,109,102,413.8,33.5,10.79,10.26,10.70,10.34,25.99,10.5,19.5,2.0,9.2,5.3,8.4,1.7,21.7,0.4,10.9,2.400,240923\989305A.WBA,49.6,60.1,0.0,0.0,10.5,18.3,20.8,0.0,0.0,2.5,21.0,23.0,0.0,0.0,2.0,26.3,35.5,0.0,0.0,9.2,36.0,41.3,0.0,0.0,5.3,41.2,49.6,0.0,0.0,8.4,49.6,51.3,0.0,0.0,1.7,51.5,54.1,0.0,0.0,2.6,58.5,58.9,0.0,0.0,0.4,54.2,60.1,0.0,0.0,5.9,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,60.1
2024/09/23,08:09:04,989306,1,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,56.0,93,0.150,3.413,28,54.9,17.9,187.9,319.8,507.7,3.648,4.65,8,24,-0.2,8.6,1.0,476,504.3,33.7,3.4,112.00,44.1,0.006,93.4,92.8,9.5,28,109,102,186.8,35.4,11.00,10.33,10.75,10.38,26.13,28.9,19.7,2.0,7.8,5.3,8.4,1.7,22.5,0.3,10.9,2.400,240923\989306A.WBA,0.0,18.3,45.4,56.0,28.9,18.7,21.2,0.0,0.0,2.5,21.4,23.4,0.0,0.0,2.0,23.6,31.4,0.0,0.0,7.8,31.8,37.1,0.0,0.0,5.3,37.0,45.4,0.0,0.0,8.4,45.4,47.1,0.0,0.0,1.7,47.3,49.9,0.0,0.0,2.6,54.4,54.7,0.0,0.0,0.3,0.0,3.6,50.0,56.0,9.6,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,56.0
2024/09/23,08:10:00,989307,1,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,56.0,93,0.150,3.399,30,54.9,18.6,187.4,319.6,507.0,3.633,4.65,8,24,-0.2,8.9,1.1,468,503.8,33.6,3.2,112.00,44.1,0.013,93.3,92.6,9.6,30,108,101,186.1,32.7,10.93,10.31,10.74,10.34,26.14,28.8,19.8,1.9,7.7,5.3,8.5,1.7,23.1,0.3,10.8,2.400,240923\989307A.WBA,0.0,18.3,45.5,56.0,28.8,18.8,21.3,0.0,0.0,2.5,21.5,23.4,0.0,0.0,1.9,23.7,31.4,0.0,0.0,7.7,31.9,37.2,0.0,0.0,5.3,37.0,45.5,0.0,0.0,8.5,45.5,47.2,0.0,0.0,1.7,47.4,50.0,0.0,0.0,2.6,54.4,54.7,0.0,0.0,0.3,0.0,3.5,50.1,56.0,9.4,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,56.0
`
	tempDir := t.TempDir()
	setDir := filepath.Join(tempDir, "DCM 16", "062b", "241018")
	err := os.MkdirAll(setDir, os.ModeTemporary)
	if err != nil {
		t.Fatalf("failed to create dir: %v", err)
	}
	tempFile, err := os.CreateTemp(setDir, "LineData_*.csv")
	if err != nil {
		t.Fatalf("unable to create temp file: %v", err)
	}
	defer func() {
		if err = tempFile.Close(); err != nil {
			t.Errorf("unable to close file: %v", err)
		}
	}()

	_, err = tempFile.WriteString(input)
	if err != nil {
		t.Fatalf("error writing data: %v", err)
	}

	csv := Csv{
		PreProcessor: []Processor{
			&ProcessorMergeColumns{
				Name:      "mergeDateTime",
				Start:     Cell{Row: 0, Column: 0},
				End:       Cell{Row: -1, Column: 2},
				Delimiter: " ",
			},
			&ProcessorFillRight{
				Name:  "fillEmptyHeaders",
				Start: Cell{Row: 0, Column: 0},
				End:   Cell{Row: 1, Column: -1},
			},
			&ProcessorMergeRows{
				Name:           "mergeHeaderAndUnits",
				StartRow:       0,
				EndRow:         2,
				Delimiter:      " ",
				TrimWhitespace: true,
			},
			&ProcessorReplaceCell{
				Name:  "setTimestamp",
				Cell:  Cell{Row: 0, Column: 0},
				Value: "@timestamp",
			},
		},
		FilePathData: []FilePathData{
			{
				CaptureRegex: `.*/(?P<dcm>.+)/(?P<die>.+)/\w+/.+.csv$`,
			},
		},
		TableLocations: []TableLocation{
			{
				Name:           "shot",
				EndCell:        Cell{-1, -1},
				TableHasHeader: true,
				ColumnDataTypes: []DataType{
					DataTypeDateTimeStyle1, DataTypeInt64, DataTypeInt64, DataTypeInt64, DataTypeInt64, DataTypeInt64, DataTypeInt64, DataTypeInt64, DataTypeInt64, DataTypeInt64, DataTypeInt64, DataTypeInt64, DataTypeInt64, DataTypeInt64, DataTypeInt64, DataTypeInt64, DataTypeInt64, DataTypeInt64, DataTypeInt64, DataTypeInt64, DataTypeInt64, DataTypeInt64, DataTypeInt64, DataTypeInt64, DataTypeInt64, DataTypeInt64, DataTypeInt64, DataTypeInt64, DataTypeInt64, DataTypeInt64, DataTypeInt64, DataTypeInt64, DataTypeInt64, DataTypeInt64, DataTypeInt64, DataTypeInt64, DataTypeInt64, DataTypeInt64, DataTypeInt64, DataTypeInt64, DataTypeInt64, DataTypeInt64, DataTypeInt64, DataTypeInt64, DataTypeInt64, DataTypeInt64, DataTypeInt64, DataTypeInt64, DataTypeInt64, DataTypeInt64, DataTypeInt64, DataTypeInt64, DataTypeFloat64, DataTypeInt64, DataTypeFloat64, DataTypeFloat64, DataTypeInt64, DataTypeFloat64, DataTypeFloat64, DataTypeFloat64, DataTypeFloat64, DataTypeFloat64, DataTypeFloat64, DataTypeFloat64, DataTypeInt64, DataTypeInt64, DataTypeFloat64, DataTypeFloat64, DataTypeFloat64, DataTypeFloat64, DataTypeFloat64, DataTypeFloat64, DataTypeFloat64, DataTypeFloat64, DataTypeFloat64, DataTypeFloat64, DataTypeFloat64, DataTypeFloat64, DataTypeFloat64, DataTypeInt64, DataTypeInt64, DataTypeInt64, DataTypeFloat64, DataTypeFloat64, DataTypeFloat64, DataTypeFloat64, DataTypeFloat64, DataTypeFloat64, DataTypeFloat64, DataTypeFloat64, DataTypeFloat64, DataTypeFloat64, DataTypeFloat64, DataTypeFloat64, DataTypeFloat64, DataTypeFloat64, DataTypeFloat64, DataTypeFloat64, DataTypeFloat64, DataTypeFloat64, DataTypeString, DataTypeFloat64, DataTypeFloat64, DataTypeFloat64, DataTypeFloat64, DataTypeFloat64, DataTypeFloat64, DataTypeFloat64, DataTypeFloat64, DataTypeFloat64, DataTypeFloat64, DataTypeFloat64, DataTypeFloat64, DataTypeFloat64, DataTypeFloat64, DataTypeFloat64, DataTypeFloat64, DataTypeFloat64, DataTypeFloat64, DataTypeFloat64, DataTypeFloat64, DataTypeFloat64, DataTypeFloat64, DataTypeFloat64, DataTypeFloat64, DataTypeFloat64, DataTypeFloat64, DataTypeFloat64, DataTypeFloat64, DataTypeFloat64, DataTypeFloat64, DataTypeFloat64, DataTypeFloat64, DataTypeFloat64, DataTypeFloat64, DataTypeFloat64, DataTypeFloat64, DataTypeFloat64, DataTypeFloat64, DataTypeFloat64, DataTypeFloat64, DataTypeFloat64, DataTypeFloat64, DataTypeFloat64, DataTypeFloat64, DataTypeFloat64, DataTypeFloat64, DataTypeFloat64, DataTypeFloat64, DataTypeFloat64, DataTypeFloat64, DataTypeFloat64, DataTypeFloat64, DataTypeFloat64, DataTypeFloat64, DataTypeFloat64, DataTypeFloat64, DataTypeFloat64, DataTypeFloat64, DataTypeFloat64, DataTypeFloat64, DataTypeFloat64, DataTypeFloat64, DataTypeFloat64, DataTypeFloat64, DataTypeFloat64, DataTypeFloat64, DataTypeFloat64, DataTypeFloat64, DataTypeFloat64, DataTypeFloat64, DataTypeFloat64,
				},
				ParseSeparated: true,
				IgnoreNesting:  true,
				SkipBlankData:  true,
			},
		},
		IdField: IdField{
			Parameters: []IdFieldParameter{
				{Mapping: []any{"dcm"}},
				{Mapping: []any{"@timestamp"}},
			},
			Delimiter: "_",
		},
	}

	output, ids, err := csv.ParseFile(tempFile, tempDir)
	if err != nil {
		t.Fatalf("error parsing file: %v", err)
	}

	if output[0]["Alarm7"] != int64(2) {
		t.Errorf("Expected output[0][\"Alarm7\"] == 2 instead got: %v", output[0]["Alarm7"] != 2)
	} else if output[4]["Hi-Vel._Acceleration_msec"] != int64(24) {
		t.Errorf("Expected output[4][\"Hi-Vel. Acceleration msec\"] == 24 instead got %v", output[4]["Hi-Vel. Acceleration msec"])
	}

	dcm, ok := output[0]["dcm"].(string)
	if !ok {
		t.Fatalf("failed interface conversion for dcm filepath data")
	}
	if dcm != "DCM 16" {
		t.Fatalf("dcm should be equal to 16")
	}

	timestamp, ok := output[0]["@timestamp"].(string)
	if !ok {
		t.Fatalf("failed interface conversion for @timestamp")
	}
	expectedTimestamp := time.Date(2024, 9, 23, 8, 4, 18, 0, time.Local).Format(time.RFC3339)
	if timestamp != expectedTimestamp {
		t.Errorf("@timestamp is incorrect\nExpected: %s\nReceived: %s", expectedTimestamp, timestamp)
	}

	if ids[0] != dcm+"_"+timestamp {
		t.Fatalf("id is incorrect\nExpected: %s\nReceived: %s", dcm+"_"+timestamp, ids[0])
	}
}

func TestCsvStyle2(t *testing.T) {
	t.Parallel()

	input := `
WaveVersion,2.00
Date,2024/09/23
Time,09:39:10
Version,D8MJ1-C9
Shot,989392
WaveSel,0,0,0,1,2
WaveName,Spd,Pres,Pos,Vac
WaveUnit,m/s,MPa,mm,KPa
WaveMax,3.609,54.6,502.1,8.1
Section
Index,Offset,StartTime,SampleTime,Count
0,0,0.0,0.0,0
1,0,0.0,20.0,244
2,0,4880.0,4.0,31
3,0,5004.0,4.0,50
4,0,5204.0,200.0,13
5,1,7804.0,74.0,2
WaveData
Time,Spd,Pres,Pos,Vac
ms,m/s,MPa,mm,KPa
0,0.000,0.3,0.8,0.0,1.6
20,0.000,0.3,0.8,0.0,1.6
40,0.000,0.3,0.8,0.0,1.6
60,0.000,0.3,0.8,0.0,1.6
80,0.000,0.3,0.8,0.0,1.6
100,0.000,0.3,0.8,0.0,1.6
120,0.000,0.3,0.8,0.0,1.6
140,0.000,0.3,0.8,0.0,1.6
160,0.000,0.3,0.8,0.0,1.6
180,0.010,0.3,0.8,0.0,1.6
200,0.193,0.2,3.8,0.0,1.6
220,0.141,0.0,7.1,0.0,1.6
`

	tempDir := t.TempDir()
	setDir := filepath.Join(tempDir, "DCM 16", "062b", "241018")
	err := os.MkdirAll(setDir, os.ModeTemporary)
	if err != nil {
		t.Fatalf("failed to create dir: %v", err)
	}
	tempFile, err := os.CreateTemp(setDir, "1234_*.csv")
	if err != nil {
		t.Fatalf("unable to create temp file: %v", err)
	}
	defer func() {
		if err = tempFile.Close(); err != nil {
			t.Errorf("unable to close file: %v", err)
		}
	}()

	_, err = tempFile.WriteString(input)
	if err != nil {
		t.Fatalf("error writing data: %v", err)
	}

	csv := Csv{
		PreProcessor: []Processor{
			&ProcessorRemoveCellLeft{
				Name: "Remove Empty Wave",
				Cell: Cell{Row: 5, Column: 5},
			},
			&ProcessorTransposeRow{
				Name:     "Transpose Wave Metrics",
				StartRow: 5,
				EndRow:   9,
			},
		},
		FilePathData: []FilePathData{
			{
				CaptureRegex: `.*/(?P<dcm>.+)/(?P<die>.+)/\w+/.+.csv$`,
			},
		},
		CellLocations: []CellLocation{
			{
				NameCell: Cell{Row: 0, Column: 0},
				Location: Cell{Row: 0, Column: 1},
				DataType: DataTypeFloat64,
			},
			{
				NameCell: Cell{Row: 3, Column: 0},
				Location: Cell{Row: 3, Column: 1},
				DataType: DataTypeString,
			},
			{
				NameCell: Cell{Row: 4, Column: 0},
				Location: Cell{Row: 4, Column: 1},
				DataType: DataTypeInt64,
			},
		},
		ConcatCellLocations: []ConcatCellLocation{
			{
				Name: "@timestamp",
				Cells: []Cell{
					{Row: 1, Column: 1},
					{Row: 2, Column: 1},
				},
				Delimiter: " ",
				DataType:  DataTypeDateTimeStyle1,
			},
		},
		TableLocations: []TableLocation{
			{
				Name:           "metrics",
				StartCell:      Cell{Row: 5, Column: 0},
				EndCell:        Cell{Row: 9, Column: 3},
				TableHasHeader: true,
				ColumnDataTypes: []DataType{
					DataTypeInt64, DataTypeString, DataTypeString, DataTypeFloat64,
				},
			},
			{
				Name:           "section",
				StartCell:      Cell{Row: 11, Column: 0},
				EndCell:        Cell{Row: 17, Column: -1},
				TableHasHeader: true,
				ColumnDataTypes: []DataType{
					DataTypeInt64, DataTypeInt64, DataTypeFloat64, DataTypeFloat64, DataTypeInt64,
				},
			},
			{
				Name:           "wave",
				StartCell:      Cell{Row: 21, Column: 0},
				EndCell:        Cell{Row: -1, Column: -1},
				TableHasHeader: false,
				HeaderNames: []string{
					"Time ms", "Speed m/s", "Pressure MPa", "Position mm", "Vac KPa", "Shot Sleeve Vac KPa",
				},
				ColumnDataTypes: []DataType{
					DataTypeInt64, DataTypeFloat64, DataTypeFloat64, DataTypeFloat64, DataTypeFloat64, DataTypeFloat64,
				},
			},
		},
		IdField: IdField{
			Parameters: []IdFieldParameter{
				{Mapping: []any{"dcm"}},
				{Mapping: []any{"@timestamp"}},
			},
			Delimiter: "_",
		},
		KeepSpaces: true,
	}

	output, ids, err := csv.ParseFile(tempFile, setDir)
	if err != nil {
		t.Fatalf("error parsing file: %v", err)
	}

	if output[0]["Version"] != "D8MJ1-C9" {
		t.Errorf(`Version should be "D8MJ1-C9 but is %v`, output[0]["Version"])
	}

	metrics, ok := output[0]["metrics"].([]map[string]any)
	if !ok {
		t.Fatalf("failed interface conversion for metrics table")
	}
	if metrics[3]["WaveMax"] != Float64(8.1) {
		t.Errorf("should be 8.1 but is %v of type %T", metrics[3]["WaveMax"], metrics[3]["WaveMax"])
	}

	wave, ok := output[0]["wave"].([]map[string]any)
	if !ok {
		t.Fatalf("failed interface conversion for wave table")
	}
	if wave[0]["Time ms"] != int64(0) {
		t.Errorf("time should be 0 Int but is %v of type %T", wave[0]["Time ms"], wave[0]["Time ms"])
	}

	dcm, ok := output[0]["dcm"].(string)
	if !ok {
		t.Fatalf("failed interface conversion for dcm filepath data")
	}
	if dcm != "DCM 16" {
		t.Errorf("dcm should be equal to 16")
	}

	timestamp, ok := output[0]["@timestamp"].(string)
	if !ok {
		t.Fatalf("failed interface conversion for @timestamp")
	}
	expectedTimestamp := time.Date(2024, 9, 23, 9, 39, 10, 0, time.Local).Format(time.RFC3339)
	if timestamp != expectedTimestamp {
		t.Errorf("@timestamp is incorrect\nExpected: %s\nReceived: %s", expectedTimestamp, timestamp)
	}

	if ids[0] != dcm+"_"+timestamp {
		t.Fatalf("id is incorrect\nExpected: %s\nReceived: %s", dcm+"_"+timestamp, ids[0])
	}

	_, err = json.Marshal(output)
	if err != nil {
		t.Fatalf("error marshalling data: %v", err)
	}
}
