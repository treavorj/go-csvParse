package csvParse

import (
	"fmt"
	"regexp"
)

type FilePathData struct {
	Name          string
	StartLocation uint16
	EndLocation   uint16

	// must have capture groups defined or no data will be found
	//
	// will be ignored if empty
	// capture group names will be used as the key
	CaptureRegex string

	regex *regexp.Regexp
}

func (f *FilePathData) Parse(filePath string) (output map[string]string, err error) {
	output = map[string]string{}

	if f.EndLocation > 0 {
		if len(filePath) <= int(f.EndLocation) {
			return nil, fmt.Errorf("filePath must be at least %d long instead of %d", f.EndLocation, len(filePath))
		}
		output[f.Name] = filePath[f.StartLocation:f.EndLocation]
	}

	if f.CaptureRegex != "" {
		if f.regex == nil {
			f.regex, err = regexp.Compile(f.CaptureRegex)
			if err != nil {
				return nil, fmt.Errorf("error compiling regexp: %w", err)
			}
		}

		match := f.regex.FindStringSubmatch(filePath)
		if len(match) > 0 {
			for i, name := range f.regex.SubexpNames() {
				if i != 0 && name != "" { // Ignore the entire match and unnamed groups
					output[name] = match[i]
				}
			}
		} else {
			return nil, fmt.Errorf("not matches found")
		}
	}

	return output, nil
}
