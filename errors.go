package sfr

import "fmt"

//ConfigurationError is a generic error to denote that the configuration is invalid.
type ConfigurationError error

//RecordParseError denotes an error processing a Record from a RecordDefinition
type RecordParseError struct {
	RecordName string
	Text       string
}

func (re RecordParseError) Error() string {
	return fmt.Sprintf("Record \"%s\": %s", re.RecordName, re.Text)
}

//FieldParseError denotes an error processing a Field from a FieldDefinition
type FieldParseError struct {
	RecordName string
	FieldName  string
	Text       string
}

func (fe FieldParseError) Error() string {
	return fmt.Sprintf("Record \"%s\", Field \"%s\": %s", fe.RecordName, fe.FieldName, fe.Text)
}
