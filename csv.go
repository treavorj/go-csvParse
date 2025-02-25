package csvParse

import (
	"encoding/csv"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"time"
)

type Csv struct {
	FilePathData        []FilePathData
	PreProcessor        []Processor
	CellLocations       []CellLocation
	ConcatCellLocations []ConcatCellLocation
	TableLocations      []TableLocation
	TimeFields          []TimeField
	IdField             IdField
	FaultOnDuplicate    bool
	KeepSpaces          bool
	StoreFileTime       bool
	FileTimeName        string
}

func NewCsvFile(cellLocations []CellLocation, concatCellLocations []ConcatCellLocation, tableLocations []TableLocation) *Csv {
	return &Csv{
		CellLocations:       cellLocations,
		ConcatCellLocations: concatCellLocations,
		TableLocations:      tableLocations,
		FaultOnDuplicate:    false,
	}
}

func (c Csv) MarshalJSON() ([]byte, error) {
	for _, processor := range c.PreProcessor {
		processor.SetType()
	}
	type Alias Csv
	return json.Marshal(Alias(c))
}

func (c *Csv) UnmarshalJSON(data []byte) error {
	type Alias Csv
	aux := &struct {
		PreProcessor []json.RawMessage
		*Alias
	}{
		Alias: (*Alias)(c),
	}
	err := json.Unmarshal(data, &aux)
	if err != nil {
		return fmt.Errorf("error unmarshaling data: %w", err)
	}

	c.PreProcessor = make([]Processor, len(aux.PreProcessor))
	var tempType struct {
		Type ProcessorType
	}
	for i, raw := range aux.PreProcessor {
		if err := json.Unmarshal(raw, &tempType); err != nil {
			return fmt.Errorf("error unMarshalling Type field: %w", err)
		}

		c.PreProcessor[i], err = getProcessor(tempType.Type, raw)
		if err != nil {
			return fmt.Errorf("unable to unmarshal processor %d with data (%v): %w", i, raw, err)
		}
	}

	return nil
}

func (c *Csv) Process(file *os.File, filepath string) (result [][]byte, id []string, err error) {
	if len(c.FilePathData) == 0 &&
		len(c.PreProcessor) == 0 &&
		len(c.CellLocations) == 0 &&
		len(c.ConcatCellLocations) == 0 &&
		len(c.TableLocations) == 0 {
		return nil, nil, fmt.Errorf("no settings to process")
	}
	res, ids, err := c.ParseFile(filepath)
	if err != nil {
		return nil, nil, fmt.Errorf("error parsing csv file %s: %w", file.Name(), err)
	}

	result = make([][]byte, len(res))
	for n, doc := range res {
		result[n], err = json.Marshal(doc)
		if err != nil {
			return nil, nil, fmt.Errorf("unable to marshal map for file %s: %w", file.Name(), err)
		}
	}

	return result, ids, nil
}

// Parse a file and return all the results grouped.
//
// Will output either a map[string]any or []map[string]any
func (c *Csv) ParseFile(filePath string) ([]map[string]any, []string, error) {
	filePathData, err := c.ParseFileNames(filePath)
	if err != nil {
		return nil, nil, fmt.Errorf("error parsing filePath: %w", err)
	}

	if c.StoreFileTime {
		if c.FileTimeName == "" {
			return nil, nil, fmt.Errorf("storeFileTime is true but not FileTimeName provided")
		}

		timeVal, err := getCreationTime(filePath)
		if err != nil {
			return nil, nil, fmt.Errorf("cannot get fileTime: %w", err)
		}

		if c.FaultOnDuplicate {
			if value, exists := filePathData[c.FileTimeName]; exists {
				return nil, nil, fmt.Errorf("fileTimeName, %s, already exists in filePathData with value %v", c.FileTimeName, value)
			}
		}
		filePathData[c.FileTimeName] = timeVal.Format(time.RFC3339)
	}

	records, err := getRecords(filePath)
	if err != nil {
		return nil, nil, fmt.Errorf("error getting records: %w", err)
	}

	for n, processor := range c.PreProcessor {
		records, err = processor.Execute(records)
		if err != nil {
			return nil, nil, fmt.Errorf("error preprocessing records with processor %d (%s): %w", n, processor.GetName(), err)
		}
	}

	output, err := c.ParseRecords(records)
	if err != nil {
		return nil, nil, fmt.Errorf("error parsing records: %w", err)
	}

	var outputData []map[string]any
	switch output := output.(type) {
	case map[string]any:
		for key, value := range filePathData {
			if c.FaultOnDuplicate {
				_, exists := output[key]
				if exists {
					return nil, nil, fmt.Errorf("%s exists in csv data. FilePath data: %v | Output data: %v", key, value, output[key])
				}
			}
			output[key] = value
		}
		outputData = []map[string]any{output}
	case []map[string]any:
		for key, value := range filePathData {
			for _, data := range output {
				if c.FaultOnDuplicate {
					_, exists := data[key]
					if exists {
						return nil, nil, fmt.Errorf("%s exists in csv data. FilePath data: %v | Output data: %v", key, value, data[key])
					}
				}
				data[key] = value
			}
		}
		outputData = output
	default:
		return nil, nil, fmt.Errorf("invalid output type: %T", output)
	}

	var ids []string
	if len(c.IdField.Parameters) > 0 {
		ids, err = c.IdField.Process(outputData)
		if err != nil {
			return nil, nil, fmt.Errorf("error processing IdField: %w", err)
		}
	}

	return outputData, ids, nil
}

func (c *Csv) ParseFileNames(filePath string) (map[string]string, error) {
	output := make(map[string]string)
	filePath = strings.ReplaceAll(filePath, "\\", "/")

	for _, filePathData := range c.FilePathData {
		data, err := filePathData.Parse(filePath)
		if err != nil {
			return nil, fmt.Errorf("error parsing %s: %w", filePathData.Name, err)
		}

		for key, value := range data {
			output[key] = value
		}
	}
	return output, nil
}

func (c *Csv) ParseRecords(records [][]string) (any, error) {
	baseData := make(map[string]any)
	// Parse Cells
	for _, cellLocation := range c.CellLocations {
		name, data, err := cellLocation.Parse(records)
		if err != nil {
			return nil, err
		}
		if !c.KeepSpaces {
			name = strings.ReplaceAll(name, " ", "_")
		}
		if c.FaultOnDuplicate {
			if _, exists := baseData[name]; exists {
				return nil, fmt.Errorf("duplicate data found for cell (%s)", name)
			}
		}
		baseData[name] = data
	}

	// Parse ConcatCells
	for _, concatCellLocation := range c.ConcatCellLocations {
		name, data, err := concatCellLocation.Parse(records)
		if err != nil {
			return nil, err
		}
		if !c.KeepSpaces {
			name = strings.ReplaceAll(name, " ", "_")
		}
		if c.FaultOnDuplicate {
			if _, exists := baseData[name]; exists {
				return nil, fmt.Errorf("duplicate data found for concatCell (%s)", name)
			}
		}
		baseData[name] = data
	}

	// Parse non-separated Tables
	for _, tableLocation := range c.TableLocations {
		if tableLocation.ParseSeparated {
			continue
		}

		tableName, tableData, err := tableLocation.Parse(records, c.KeepSpaces)
		if err != nil {
			return nil, fmt.Errorf("error parsing table (%s): %w", tableName, err)
		}
		if c.FaultOnDuplicate {
			if _, exists := baseData[tableName]; exists {
				return nil, fmt.Errorf("duplicate data found for table (%s)", tableName)
			}
		}
		baseData[tableName] = tableData
	}

	// Parse Timestamp
	for _, timeField := range c.TimeFields {
		if !timeField.IsBlank() {
			continue
		}

		timestamp, err := timeField.Parse(records)
		if err != nil {
			return nil, err
		}
		if c.FaultOnDuplicate {
			if value, exists := baseData[timeField.Name]; exists {
				return nil, fmt.Errorf("duplicate key found for @timestamp with value: %v", value)
			}
		}
		baseData[timeField.Name] = timestamp.Format(time.RFC3339)
	}

	var csvData []map[string]any
	// Parse separated tables
	for _, tableLocation := range c.TableLocations {
		if !tableLocation.ParseSeparated {
			continue
		}

		if tableLocation.ParseAsArray || tableLocation.ParseSingleRow {
			return nil, fmt.Errorf("invalid options selection. Cannot parse as array or single row if parsing separated for table, %s", tableLocation.Name)
		}

		tableName, tableData, err := tableLocation.Parse(records, c.KeepSpaces)
		if err != nil {
			return nil, fmt.Errorf("error parsing table (%s): %w", tableName, err)
		}
		if c.FaultOnDuplicate {
			if _, exists := baseData[tableName]; exists {
				return nil, fmt.Errorf("duplicate data found for table (%s)", tableName)
			}
		}

		switch tableData := tableData.(type) {
		case []map[string]any:
			data := tableData
			for _, row := range data {
				instance := make(map[string]any, len(baseData)+len(row))
				for key, value := range baseData {
					instance[key] = value
				}
				if tableLocation.IgnoreNesting {
					for key, value := range row {
						instance[key] = value
					}
				} else {
					instance[tableLocation.Name] = row
				}
				csvData = append(csvData, instance)
			}
		default:
			return nil, fmt.Errorf("table is of wrong type: %T", tableData)
		}
	}

	if len(csvData) == 0 {
		return baseData, nil
	}

	return csvData, nil
}

// parse a file and return all results per type of search
func (c *Csv) ParseRecordsSegmented(records [][]string) (cells map[string]any, concatCells map[string]any, tables map[string]any, timestamps map[string]time.Time, err error) {
	// Parse Cells
	Cells := make(map[string]any)
	for _, cellLocation := range c.CellLocations {
		name, data, err := cellLocation.Parse(records)
		if err != nil {
			return nil, nil, nil, nil, err
		}
		if !c.KeepSpaces {
			name = strings.ReplaceAll(name, " ", "_")
		}
		if c.FaultOnDuplicate {
			if _, exists := Cells[name]; exists {
				return nil, nil, nil, nil, fmt.Errorf("duplicate data found for cell (%s)", name)
			}
		}
		Cells[name] = data
	}

	// Parse ConcatCells
	ConcatCells := make(map[string]any)
	for _, concatCellLocation := range c.ConcatCellLocations {
		name, data, err := concatCellLocation.Parse(records)
		if err != nil {
			return nil, nil, nil, nil, err
		}
		if !c.KeepSpaces {
			name = strings.ReplaceAll(name, " ", "_")
		}
		if c.FaultOnDuplicate {
			if _, exists := ConcatCells[name]; exists {
				return nil, nil, nil, nil, fmt.Errorf("duplicate data found for concatCell (%s)", name)
			}
		}
		ConcatCells[name] = data
	}

	// Parse Tables
	Tables := make(map[string]any)
	for _, tableLocation := range c.TableLocations {
		tableName, tableData, err := tableLocation.Parse(records, c.KeepSpaces)
		if err != nil {
			return nil, nil, nil, nil, err
		}
		if c.FaultOnDuplicate {
			if _, exists := Tables[tableName]; exists {
				return nil, nil, nil, nil, fmt.Errorf("duplicate data found for table (%s)", tableName)
			}
		}
		Tables[tableName] = tableData
	}

	// Parse Timestamp
	timeFields := make(map[string]time.Time)
	for _, timeField := range c.TimeFields {
		if !timeField.IsBlank() {
			continue
		}

		timestamp, err := timeField.Parse(records)
		if err != nil {
			return nil, nil, nil, nil, err
		}

		if c.FaultOnDuplicate {
			if value, exists := timeFields[timeField.Name]; exists {
				return nil, nil, nil, nil, fmt.Errorf("duplicate key found for timeField %s with value %v", timeField.Name, value)
			}
		}
		timeFields[timeField.Name] = timestamp
	}

	return Cells, ConcatCells, Tables, timeFields, nil
}

func getRecords(filePath string) ([][]string, error) {
	file, err := os.Open(filePath)
	if err != nil {
		return nil, fmt.Errorf("error opening file at path %s: %w", filePath, err)
	}
	defer file.Close()

	_, err = file.Seek(0, 0)
	if err != nil {
		return nil, fmt.Errorf("error seeking to beginning of file: %w", err)
	}

	csvReader := csv.NewReader(file)
	csvReader.FieldsPerRecord = -1

	// Read all records
	records, err := csvReader.ReadAll()
	if err != nil {
		return nil, fmt.Errorf("error reading records: %w", err)
	}
	if len(records) == 0 {
		return nil, fmt.Errorf("no records found")
	}
	return records, nil
}

func findValue(cell Cell, records [][]string) (string, error) {
	if cell.Row >= len(records) {
		return "", fmt.Errorf("row out of bounds. maxRow=%d, requestedRow=%d", len(records), cell.Row)
	}
	if cell.Column >= len(records[cell.Row]) {
		return "", fmt.Errorf("column out of bound. maxColumn=%d, requestedColumn=%d", len(records[cell.Row]), cell.Column)
	}
	return records[cell.Row][cell.Column], nil
}
