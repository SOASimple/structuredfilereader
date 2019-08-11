package structuredfilereader

import (
	"encoding/json"
	"fmt"
)

const fixedWidthRecordReaderName = "FixedWidth"

func init() {
	RecordReaderRegistry[fixedWidthRecordReaderName] = RecordReaderUnmarshalFunc(func(data []byte) (RecordReader, error) {
		var rr FixedWidthRecordReader
		err := json.Unmarshal(data, &rr)
		if err != nil {
			return nil, fmt.Errorf("Unmarshalling FixedWidthRecordReader failed: %s", err)
		}
		return rr, nil
	})
}

//FixedWidthRecordReader reads records into arrays of strings.
type FixedWidthRecordReader struct {
	Coordinates []FixedWidthFieldCoordinate
}

//Read splits record based on the configured Coordinates.
func (fwr FixedWidthRecordReader) Read(data []byte) (values []string, err error) {
	values = make([]string, len(fwr.Coordinates))
	for i, coord := range fwr.Coordinates {
		values[i] = string(data[coord.Start:coord.End])
	}
	return values, nil
}

//FixedWidthFieldCoordinate defines the start & end indices of a field within a record,
type FixedWidthFieldCoordinate struct {
	Start int
	End   int
}
