package structuredfilereader

import (
	"encoding/json"
	"fmt"
	"math"
	"strconv"
	"time"
)

/////////
//STRING
/////////
func init() {
	FieldTypeRegistry["String"] = FieldTypeUnmarshalFunc(func(data []byte) (FieldType, error) {
		return StringFieldType{}, nil
	})
}

//StringFieldType is a FieldType which produces Fields containing string values.
type StringFieldType struct{}

//GetValue returns a field containing a string value.
func (sft StringFieldType) GetValue(data string) (interface{}, error) {
	return data, nil
}

/////////
//NUMBER
/////////
func init() {
	FieldTypeRegistry["Number"] = FieldTypeUnmarshalFunc(func(data []byte) (numFieldType FieldType, err error) {
		nft := NumberFieldType{}
		if len(data) > 0 {
			err = json.Unmarshal(data, &nft)
		}
		numFieldType = nft
		return
	})
}

//NumberFieldType is a FieldType which produces Fields containing float64 values.
type NumberFieldType struct {
	ConvertToDecimalPlaces int
}

//GetValue returns a field containing a float64 value.
func (nft NumberFieldType) GetValue(data string) (interface{}, error) {
	val, err := strconv.ParseFloat(data, 64)
	if err != nil {
		return nil, err
	}
	return val / math.Pow(10, float64(nft.ConvertToDecimalPlaces)), nil
}

/////////
//DATE
/////////
func init() {
	FieldTypeRegistry["Date"] = FieldTypeUnmarshalFunc(func(data []byte) (FieldType, error) {
		var rawType map[string]json.RawMessage
		err := json.Unmarshal(data, &rawType)
		if err != nil {
			return nil, fmt.Errorf("Missing FieldType for Date: %s", err)
		}
		if _, ok := rawType["Format"]; !ok {
			return nil, fmt.Errorf("Format is required for Date FieldTypes")
		}
		var dft DateFieldType
		err = json.Unmarshal(data, &dft)
		return dft, err
	})
}

//DateFieldType is a FieldType which produces Fields containing float64 values.
type DateFieldType struct {
	Format string
}

//GetValue returns a field containing a time.Time value.
func (dft DateFieldType) GetValue(data string) (interface{}, error) {
	return time.Parse(dft.Format, data)
}
