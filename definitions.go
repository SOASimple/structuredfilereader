package sfr

import (
	"encoding/json"
	"fmt"
	"regexp"
)

//RecordReaderUnmarshalFunc is an implementation-provided function to unmarshal
//a RecordReader.
type RecordReaderUnmarshalFunc func(data []byte) (RecordReader, error)

//RecordReaderRegistry holds a reference of RecordReader names to json.Unmarshaler
//instances which can produce the correct concrete implementation of RecordReader.
//This registry should be populated by RecordReader implementations in their
//init() functions.
var RecordReaderRegistry = make(map[string]RecordReaderUnmarshalFunc)

//GetRecordReaderUnmarshalFunc returns a RecordReaderUnmarshalFunc
//which registered itself with the given name.
func GetRecordReaderUnmarshalFunc(name string) (RecordReaderUnmarshalFunc, error) {
	readerFunc, ok := RecordReaderRegistry[name]
	if !ok {
		return nil, ConfigurationError(fmt.Errorf("No RecordReader named \"%s\" exists in registry", name))
	}
	return readerFunc, nil
}

//FieldTypeUnmarshalFunc is an implementation-provided function to unmarshal
//a FieldType.
type FieldTypeUnmarshalFunc func(data []byte) (FieldType, error)

//FieldTypeRegistry holds a reference of FieldType names to json.Unmarshaler
//instances which can produce the right concrete implementation of FieldType.
//This registry should be populated by FieldType implementations in their
//init() functions.
var FieldTypeRegistry = make(map[string]FieldTypeUnmarshalFunc)

//GetFieldTypeUnmarshalFunc returns a FieldTypeUnmarshalFunc
//which registered itself with the given name.
func GetFieldTypeUnmarshalFunc(name string) (FieldTypeUnmarshalFunc, error) {
	typeFunc, ok := FieldTypeRegistry[name]
	if !ok {
		return nil, ConfigurationError(fmt.Errorf("No FieldType named \"%s\" exists in registry", name))
	}
	return typeFunc, nil
}

//RecordDefinition objects contain FielDefinitions and join information for
//structuring related Records from the file.
type RecordDefinition struct {
	Name             string
	MatchExpression  string
	ReaderName       string
	RecordReader     RecordReader
	ParentRecordName string
	FieldDefinitions []FieldDefinition
}

//Match matches the current record against the regular expression configured
//for this RecordDefinition. If a regexp is configured in the RecordDefinition,
//it returns the result of regexp.Match. If no regexp is configured in the
//RecordDefinitnion, it always returns true.
func (rd *RecordDefinition) Match(data []byte) (bool, error) {
	if rd.MatchExpression == "" {
		return true, nil
	}
	return regexp.Match(rd.MatchExpression, data)
}

//UnmarshalJSON unmarshals a RecordDefinition from JSON.
//This is required to select the correct RecordReader implementation
//from the RecordReaderRegistry.
func (rd *RecordDefinition) UnmarshalJSON(data []byte) error {
	var rawRecDef map[string]json.RawMessage
	err := json.Unmarshal(data, &rawRecDef)
	if err != nil {
		return err
	}

	//Name
	err = mustUnmarshalString(rawRecDef, "Name", &rd.Name)
	if err != nil {
		return err
	}

	//MatchExpression
	err = unmarshalString(rawRecDef, "MatchExpression", &rd.MatchExpression)
	if err != nil {
		return err
	}

	//recordReader
	var readerName string
	err = mustUnmarshalString(rawRecDef, "ReaderName", &readerName)
	if err != nil {
		return err
	}
	rawReader, ok := rawRecDef["RecordReader"]
	if !ok {
		return fmt.Errorf("No RecordReader defined in RecordDefinition \"%s\"", rd.Name)
	}
	var rawReaderFields map[string]json.RawMessage
	err = json.Unmarshal(rawReader, &rawReaderFields)
	if err != nil {
		return err
	}
	readerFunc, err := GetRecordReaderUnmarshalFunc(readerName)
	if err != nil {
		return fmt.Errorf("Error getting record reader unmarshal function for RecordDefinition \"%s\": %s", rd.Name, err)
	}
	rd.RecordReader, err = readerFunc(rawReader)

	//ParentRecordName
	err = unmarshalString(rawRecDef, "ParentRecordName", &rd.ParentRecordName)
	if err != nil {
		return err
	}

	//FieldDefinitions
	if rawFieldDefinitions, ok := rawRecDef["FieldDefinitions"]; ok {
		err = json.Unmarshal(rawFieldDefinitions, &rd.FieldDefinitions)
		if err != nil {
			return err
		}
	}
	return err
}

//FieldDefinition is a named instance of a FieldType
type FieldDefinition struct {
	Name      string
	TypeName  string
	FieldType FieldType
}

//UnmarshalJSON builds a FieldDefinition using a registered FieldTypeUnmarshalFunc.
func (def *FieldDefinition) UnmarshalJSON(data []byte) error {
	var rawFieldDef map[string]json.RawMessage
	err := json.Unmarshal(data, &rawFieldDef)
	if err != nil {
		return err
	}

	//Name
	err = mustUnmarshalString(rawFieldDef, "Name", &def.Name)
	if err != nil {
		return err
	}

	//TypeName
	err = mustUnmarshalString(rawFieldDef, "TypeName", &def.TypeName)
	if err != nil {
		return err
	}
	rawType := rawFieldDef["FieldType"]
	typeFunc, err := GetFieldTypeUnmarshalFunc(def.TypeName)
	if err != nil {
		return fmt.Errorf("Error getting field type unmarshal function for FieldType \"%s\": %s", def.Name, err)
	}
	def.FieldType, err = typeFunc(rawType)

	return err
}

//FieldType defines a type of Field (String, Date, Number, etc)
type FieldType interface {
	GetValue(data string) (interface{}, error)
}
