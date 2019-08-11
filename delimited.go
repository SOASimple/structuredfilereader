package structuredfilereader

import (
	"encoding/csv"
	"encoding/json"
	"fmt"
	"strings"
)

const delimitedRecordReaderName = "Delimited"

func init() {
	RecordReaderRegistry[delimitedRecordReaderName] = func(data []byte) (RecordReader, error) {
		var dr DelimitedRecordReader
		err := json.Unmarshal(data, &dr)
		if err != nil {
			return nil, fmt.Errorf("Unmarshalling DelimitedRecordReader failed: %s", err)
		}
		return dr, nil
	}
}

//DelimitedRecordReader reads records into arrays of strings.
type DelimitedRecordReader struct {
	Delimiter string
}

//Read splits record based on the configured Coordinates.
func (dr DelimitedRecordReader) Read(data []byte) (values []string, err error) {
	csvr := csv.NewReader(strings.NewReader(string(data)))
	csvr.Comma = []rune(dr.Delimiter)[0]
	return csvr.Read()
}
