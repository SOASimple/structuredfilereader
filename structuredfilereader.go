package structuredfilereader

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"regexp"
)

//logger to write logs to (defaults to Dev/Null).
var logger *log.Logger

func init() {
	f, _ := os.Open(os.DevNull)
	SetLogOutput(f)
}

//SetLogOutput sets logging for this package to the provided writer
func SetLogOutput(w io.Writer) {
	logger = log.New(w, "", log.Lshortfile+log.Lmicroseconds)
}

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
		return nil, fmt.Errorf("No RecordReader named \"%s\" exists in registry", name)
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
		return nil, fmt.Errorf("No FieldType named \"%s\" exists in registry", name)
	}
	return typeFunc, nil
}

//Parser reads a file using a RecordReader and FieldDefinitions.
type Parser struct {
	RecordDefinitions []*RecordDefinition
}

//NewParser returns a Parser using the JSON configuration read from r.
func NewParser(config io.ReadCloser) (parser Parser, err error) {
	defer config.Close()
	err = json.NewDecoder(config).Decode(&parser)
	return
}

//ParseFile parses the requested file returning 2 channels which will be written to by Parse.
//Elem can be any number of string required to build up the full path to the file
//(see http://godoc.org/path/filepath#Join).
func (p *Parser) ParseFile(elem ...string) (recordChan <-chan *Record, errorChan <-chan *error) {
	recChan := make(chan *Record)
	errChan := make(chan *error)
	//Open the file
	file, err := os.Open(filepath.Join(elem...))
	if err != nil {
		errChan <- &err
		close(recChan)
		return
	}
	return p.Parse(file)
}

//Parse writes to the first channel each time a top Level (with no parent defined) record is completed(along with its child Records).
//Callers should loop over a select on the records & err channels & exit the loop when Record.IsValid is not true.
func (p *Parser) Parse(source io.ReadCloser) (recordChan <-chan *Record, errorChan <-chan *error) {
	recChan := make(chan *Record)
	errChan := make(chan *error)
	go p.parse(source, recChan, errChan)
	return recChan, errChan
}

func (p *Parser) parse(source io.ReadCloser, recordChan chan<- *Record, errChan chan<- *error) {
	defer source.Close()
	defer close(recordChan)
	defer close(errChan)

	scanner := bufio.NewScanner(source)
	scanner.Split(bufio.ScanLines)
	//topRec holds the current top level record which children may be attached to
	//if they join. Once a new topRec is identified, the current one is sent & then replaced.
	var topRec *Record
	//For each record definition name, we remember the last record we created of that
	//name and store it here so we can attach child records which join.
	lastRecords := make(map[string]*Record)
	lineNum := 0
	for scanner.Scan() {
		lineNum++
		for _, recDef := range p.RecordDefinitions {
			match, err := recDef.Match(scanner.Bytes())
			if err != nil {
				logger.Printf("Sending error: %s", err.Error())
				errChan <- &err
				return
			}
			if !match {
				//skip this iteration & try the next RecordDefinition
				//logger.Printf("No match for record definition %s: %s\n", recDef.Name, string(scanner.Bytes()))
				continue
			}
			logger.Printf("Found match on line %d for record definition %s: %s\n", lineNum, recDef.Name, string(scanner.Bytes()))
			recVals, err := recDef.RecordReader.Read(scanner.Bytes())
			if err != nil {
				err = fmt.Errorf("Error reading from RecordReader: %s", err)
				logger.Printf("Sending error: %s", err)
				errChan <- &err
				return
			}
			rec := Record{
				Name:     recDef.Name,
				Fields:   make([]Field, 0),
				Children: make([]*Record, 0),
			}
			for i, fldDef := range recDef.FieldDefinitions {
				if i > len(recVals)-1 {
					err = fmt.Errorf("Field %s in Record %s is past the end of available data on line %d", fldDef.Name, recDef.Name, lineNum)
					logger.Printf("Sending error: %s", err.Error())
					errChan <- &err
					return
				}
				fldVal, valerr := fldDef.FieldType.GetValue(recVals[i])
				if valerr != nil {
					err = fmt.Errorf("Error on line %d getting field value: %s", lineNum, valerr)
					logger.Printf("Sending error %s\n", err)
					errChan <- &err
					return
				}
				fld := Field{
					Name:     fldDef.Name,
					TypeName: fldDef.TypeName,
					Value:    fldVal,
				}
				rec.Fields = append(rec.Fields, fld)
			}
			logger.Printf("Adding %s to list of last records", rec.Name)
			lastRecords[rec.Name] = &rec

			if recDef.ParentRecordName == "" {
				//This is a top level record, if there is already a top level record in
				//topRec, send it. Then write the current record to topRec.
				if topRec != nil {
					logMsg, _ := json.Marshal(topRec)
					logger.Printf("Sending top level record: %s = %s\n", topRec.Name, string(logMsg))
					recordChan <- topRec
				}
				logMsg, _ := json.Marshal(rec)
				logger.Printf("Setting new top level record: %s", logMsg)
				topRec = &rec
			} else {
				//This record needs to be attached to a parent
				logger.Printf("List of Last Records is %s", lastRecords)
				if parent, ok := lastRecords[recDef.ParentRecordName]; ok {
					logger.Printf("Adding %s to %s", recDef.Name, parent.Name)
					parent.Children = append(parent.Children, &rec)
				} else {
					err = fmt.Errorf("No available parent record %s for child record %s on line %d", recDef.ParentRecordName, rec.Name, lineNum)
					logger.Printf("Sending error %s\n", err)
					errChan <- &err
					return
				}
			}
			//break out to scan next line (don't loop over further RecordDefinitions)
			break
		}
	}
	//Finally, we need to send topRec.
	logger.Printf("Sending final top level record %s\n", topRec.Name)
	recordChan <- topRec
}

//ProcessorFunc defines a callback which can be passed to Process & ProcessFile.
//The callback will be called once for each top level Record read from the input
//containing the top leve Record and any of its children.
type ProcessorFunc func(record *Record, err error)

//ProcessFile will Parse the file identified  and call the passed processor once for each
//top level Record found or any error while reading the file.
//Elem can be any number of strings required to build up the full path to the file
//(see http://godoc.org/path/filepath#Join).
func (p *Parser) ProcessFile(processor ProcessorFunc, elem ...string) {
	file, err := os.Open(filepath.Join(elem...))
	if err != nil {
		processor(nil, err)
		return
	}
	p.Process(processor, file)
}

//Process will Parse the source and call the passed processor once for each
//top level Record found or any error while reading the source.
func (p *Parser) Process(processor ProcessorFunc, source io.ReadCloser) {
	recChan, errChan := p.Parse(source)
channelListener:
	for {
		select {
		case err := <-errChan:
			if err == nil {
				logger.Println("Received nil Record (on error channel)- exiting.")
				break channelListener
			}
			processor(nil, *err)
		case rec := <-recChan:
			if rec == nil {
				logger.Println("Received nil Record (on record channel)- exiting.")
				break channelListener
			}
			processor(rec, nil)
		}
	}
}

//RecordReader implementations read records into slices of strings
type RecordReader interface {
	Read(data []byte) (values []string, err error)
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

//Record objects contain all of the Fields which form a record & any child
//Records through defined joins.
type Record struct {
	Name     string
	Fields   []Field
	Children []*Record
}

//FindRecord returns a pointer to the  record with a Name matching
//recordName and, if present all Fields in jmatches with the same name,
//type & value. It will search recursively through its children & will return
//nil if no matching record is found.
func (rec *Record) FindRecord(recordName string, matches []Field) *Record {
	//First check if this record matches.
	if rec.Name == recordName {
		matchedKeys := true
		for _, match := range matches {
			fld, err := rec.GetField(match.Name)
			if err != nil {
				return nil
			}
			if fld != match {
				matchedKeys = false
				break
			}
		}
		if matchedKeys {
			return rec
		}
	}
	//If not, loop through the children & return the first non-null *Record returned.
	for _, child := range rec.Children {
		parent := child.FindRecord(recordName, matches)
		if parent != nil {
			return parent
		}
	}
	return nil
}

//GetField returns a Field from the Record or an error if no Field with that
//name exists.
func (rec *Record) GetField(name string) (Field, error) {
	for _, fld := range rec.Fields {
		if fld.Name == name {
			return fld, nil
		}
	}
	return Field{}, fmt.Errorf("No Field named \"%s\" in Record \"%s\"", name, rec.Name)
}

//Field objects are produced by FieldDefinitions & contain data converted into
//a suitable Go object type.
type Field struct {
	Name     string
	TypeName string
	Value    interface{}
}
