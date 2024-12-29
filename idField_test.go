package csvParse

import (
	"encoding/json"
	"testing"
)

func TestIdField(t *testing.T) {
	t.Parallel()

	baseData := []map[string]any{
		{
			"test1": 1,
			"test2": "hello",
			"test3": []any{
				"test", map[string]any{
					"internalTest": "success1",
				},
			},
			"test4": "world",
		},
		{
			"test1": 2,
			"test2": "foo",
			"test3": []any{
				"test", map[string]any{
					"internalTest": "success2",
				},
			},
		},
	}

	type TestStruct struct {
		IdField  IdField
		Expected []string
		Succeed  bool
	}

	tests := []TestStruct{
		{
			IdField: IdField{
				Parameters: []IdFieldParameter{
					{Mapping: []any{"test1"}},
				},
			},
			Succeed:  true,
			Expected: []string{"1", "2"},
		},
		{
			IdField: IdField{
				Parameters: []IdFieldParameter{
					{Mapping: []any{"test100"}},
				},
			},
			Succeed: false,
		},
		{
			Succeed: false,
		},
		{
			IdField: IdField{
				Parameters: []IdFieldParameter{
					{Mapping: []any{"test1"}},
					{Mapping: []any{"test2"}},
				},
			},
			Succeed:  true,
			Expected: []string{"1hello", "2foo"},
		},
		{
			IdField: IdField{
				Parameters: []IdFieldParameter{
					{Mapping: []any{"test1"}},
					{Mapping: []any{"test2"}},
				},
				Delimiter: "_",
			},
			Succeed:  true,
			Expected: []string{"1_hello", "2_foo"},
		},
		{
			IdField: IdField{
				Parameters: []IdFieldParameter{
					{Mapping: []any{"test1"}},
					{Mapping: []any{"test3"}},
				},
				Delimiter: "_",
			},
			Succeed: true,
			Expected: []string{
				"1_[test map[internalTest:success1]]",
				"2_[test map[internalTest:success2]]"},
		},
		{
			IdField: IdField{
				Parameters: []IdFieldParameter{
					{Mapping: []any{"test1"}},
					{Mapping: []any{"test3", 1}},
				},
				Delimiter: "_",
			},
			Succeed: true,
			Expected: []string{
				"1_map[internalTest:success1]",
				"2_map[internalTest:success2]",
			},
		},
		{
			IdField: IdField{
				Parameters: []IdFieldParameter{
					{Mapping: []any{"test1"}},
					{Mapping: []any{"test3", 1, "internalTest"}},
				},
				Delimiter: "_",
			},
			Succeed: true,
			Expected: []string{
				"1_success1",
				"2_success2",
			},
		},
	}

	for n, test := range tests {
		data, err := testDeepCopyArrayOfMaps(baseData)
		if err != nil {
			t.Fatalf("deep copy should not have failed: %v", err)
		}

		ids, err := test.IdField.Process(data)
		if err != nil {
			if test.Succeed {
				t.Errorf("Test %d: error while processing: %v", n, err)
				continue
			} else {
				continue
			}
		}

		for i := range data {
			if ids[i] != test.Expected[i] {
				t.Errorf("Test %d: index %d does not match\nExpected: %s\nReceived: %s", n, i, test.Expected[i], ids[i])
			}
		}
	}
}

func testDeepCopyArrayOfMaps(input []map[string]any) ([]map[string]any, error) {
	var copy []map[string]any
	data, err := json.Marshal(input)
	if err != nil {
		return nil, err
	}
	if err := json.Unmarshal(data, &copy); err != nil {
		return nil, err
	}
	return copy, nil
}
