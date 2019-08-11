package structuredfilereader

import (
	"encoding/json"
	"fmt"
)

func unmarshalString(rawMap map[string]json.RawMessage, fieldName string, target *string) error {
	err := mustUnmarshalString(rawMap, fieldName, target)
	if err != nil {
		if _, ok := err.(MissingFieldError); !ok {
			//This is not a MissingFieldError so rethrow
			return err
		}
	}
	return nil
}

func mustUnmarshalString(rawMap map[string]json.RawMessage, fieldName string, target *string) error {
	rawName, ok := rawMap[fieldName]
	if !ok {
		return MissingFieldError{Message: fmt.Sprintf("Error unmarshalling missing required field \"%s\"", fieldName)}
	}
	err := json.Unmarshal(rawName, target)
	if err != nil {
		return err
	}
	return nil
}

//MissingFieldError represent a failure to find the requested field to Unmarshal
type MissingFieldError struct {
	Message string
}

func (mfe MissingFieldError) Error() string {
	return mfe.Message
}
