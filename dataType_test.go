package csvParse

import (
	"encoding/json"
	"reflect"
	"testing"
)

func TestFloat64Marshalling(t *testing.T) {
	t.Parallel()

	data, err := json.Marshal(Float64(0))
	if err != nil {
		t.Errorf("error while marshalling 0: %v", err)
	} else if reflect.DeepEqual(data, `{0.0}`) {
		t.Errorf("expected: `{0.0}`\nreceived: %v", data)
	}
	data, err = json.Marshal(Float64(10))
	if err != nil {
		t.Errorf("error while marshalling 0: %v", err)
	} else if reflect.DeepEqual(data, `{10.0}`) {
		t.Errorf("expected: `{10.0}`\nreceived: %v", data)
	}
	data, err = json.Marshal(Float64(10.513))
	if err != nil {
		t.Errorf("error while marshalling 0: %v", err)
	} else if reflect.DeepEqual(data, `{10.513}`) {
		t.Errorf("expected: `{10.513}`\nreceived: %v", data)
	}
}
