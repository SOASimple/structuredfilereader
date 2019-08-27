package sfr

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
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

//Parser reads a file using a RecordReader and FieldDefinitions.
//SplitOnRecordName must be provided. It tells the Parser when to call the
//RecordProcessor. Each time a new Record matching that name (or EOF) is found,
//the Parser will call the RecordProcessor with the previous Record with a
//matching name (children are appended beneath the previoud record so it is only
//sent when the next record is found).
//You provide a RecordProcessor to do somthing with each Record once it is built.
//You provide an ErrorHandler to analyse each failure to determine if you wish to
//halt processing or take some other action (on a Record by Record, Field by Field basis).
//Default behaviour (if you pass nil to these) is:
//The default RecordProcessor will do nothing.
//The default ErrorHandler will terminate on all errors.
type Parser struct {
	RecordDefinitions []*RecordDefinition
	SplitOnRecordName string
	RecordProcessor   RecordProcessor
	ErrorHandler      ErrorHandler
}

//NewParser returns a Parser using the JSON configuration read from r.
func NewParser(config io.ReadCloser, processor RecordProcessor, handler ErrorHandler) (parser Parser, err error) {
	defer config.Close()
	err = ConfigurationError(json.NewDecoder(config).Decode(&parser))
	if err != nil {
		return
	}
	if processor == nil {
		parser.RecordProcessor = func(record *Record) error { return nil }
	} else {
		parser.RecordProcessor = processor
	}
	if handler == nil {
		parser.ErrorHandler = DefaultErrorHandler
	} else {
		parser.ErrorHandler = handler
	}
	return
}

//ParseFile opens the provided file & calls Parse to process the file.
//Elem can be any number of string required to build up the full path to the file
//(see http://godoc.org/path/filepath#Join).
func (p *Parser) ParseFile(elem ...string) error {
	//Open the file
	file, err := os.Open(filepath.Join(elem...))
	if err != nil {
		return err
	}
	return p.Parse(file)
}

//Parse parses the requested file calling the Parsers RecordProcessor  for each
//occurence of a record whose name matches SplitOnRecordName. It also calls the
//Parsers ErrorHandler if an error is encountered.
//Three types of error may be returned:
//1. A ConfigurationError is returned if a RecordDefinition's MatchExpression is
//   invalid.
//2. A RecordParsError is returned if the record cannot be processed -
//   ie. The Record data cannot be split into the requisite number of Fields.
//3. A FieldParseError is returned if a Field cannot be processed such as when
//   a Field is defined as numeric but does not contain numeric data.
//The last 2 error types contain struct fields containing the RecordName &
//FieldName which caused the error. This means that if a custom ErrorHandler is
//provided to the Parser, it can ignore errors on certain Records / Fields.
func (p *Parser) Parse(source io.ReadCloser) error {
	defer source.Close()
	scanner := bufio.NewScanner(source)
	scanner.Split(bufio.ScanLines)
	//splitRec holds the current record with the name SplitOnRecordName.
	//When this changes (when we are about to write a new Record to this variable)
	//we need to call the callback with this value first.
	var splitRec *Record
	//For each record definition name, we remember the last record we created of that
	//name and store it here so we can attach child records which join.
	lastRecords := make(map[string]*Record)
	lineNum := 0
	for scanner.Scan() {
		lineNum++
		for _, recDef := range p.RecordDefinitions {
			match, err := recDef.Match(scanner.Bytes())
			if err = p.ErrorHandler(ConfigurationError(err)); err != nil {
				return err
			}
			if !match {
				//skip this iteration & try the next RecordDefinition
				continue
			}
			recVals, err := recDef.RecordReader.Read(scanner.Bytes())
			if err != nil {
				err = RecordParseError{Text: fmt.Sprintf("Error reading from RecordReader: %s", err), RecordName: recDef.Name}
				if err = p.ErrorHandler(err); err != nil {
					return err
				}
			}
			rec := Record{
				Name:     recDef.Name,
				Fields:   make([]Field, 0),
				Children: make([]*Record, 0),
			}
			for i, fldDef := range recDef.FieldDefinitions {
				if i > len(recVals)-1 {
					err = RecordParseError{Text: fmt.Sprintf("Past the end of available data on line %d", lineNum), RecordName: recDef.Name}
					if err = p.ErrorHandler(err); err != nil {
						return err
					}
				}
				fldVal, valerr := fldDef.FieldType.GetValue(recVals[i])
				if valerr != nil {
					err = FieldParseError{
						Text:       fmt.Sprintf("Error on line %d getting field value: %s", lineNum, valerr),
						RecordName: recDef.Name,
						FieldName:  fldDef.Name,
					}
					if err = p.ErrorHandler(err); err != nil {
						return err
					}
				}
				fld := Field{
					Name:     fldDef.Name,
					TypeName: fldDef.TypeName,
					Value:    fldVal,
				}
				rec.Fields = append(rec.Fields, fld)
			}
			lastRecords[rec.Name] = &rec

			if recDef.Name == p.SplitOnRecordName {
				//This is a record we want to split on, if there is already a SplitRec
				//set, we need to call the callback to clear the way for the new Record.
				if splitRec != nil {
					if err = p.RecordProcessor(splitRec); err != nil {
						return err
					}
				}
				splitRec = &rec
				//Mark this record as within the split so that it will recieve children.
				rec.isWithinSplit = true
			}
			//This record needs to be attached to a parent
			if parent, ok := lastRecords[recDef.ParentRecordName]; ok {
				if parent.isWithinSplit {
					//If the parent is above the split in the hierarchy, we don't want to
					//record its children as this will mean building the entire record hierarchy
					//in referencable memory so the garbage collector won't be able to recover
					//previously sent child records.
					rec.isWithinSplit = true
					parent.Children = append(parent.Children, &rec)
				} else {
					//If the parent is within the split, we add this record to the Children
					//of the identified parent but we don't add the parent to the current
					//record as this creates a circular reference. Basically, the parent /
					//child relationships always fan out from the SplitOnRecordName.
					rec.Parent = lastRecords[recDef.ParentRecordName]
				}
			} else {
				if recDef.ParentRecordName != "" {
					err = RecordParseError{Text: fmt.Sprintf("No available parent record \"%s\" on line %d", recDef.ParentRecordName, lineNum), RecordName: recDef.Name}
					if err = p.ErrorHandler(err); err != nil {
						return err
					}
				}
			}
			//break out to scan next line (don't loop over further RecordDefinitions)
			break
		}
	}
	//Finally, send the last split record we have.
	return p.ErrorHandler(p.RecordProcessor(splitRec))
}

//RecordProcessor defines a callback configured in the Parser.
//This callback will be called once for each top level Record read from the input
//containing the current Record. Returning an error aborts processing.
type RecordProcessor func(record *Record) error

//ErrorHandler defines a callback configured in the parser.
//This callback will be called whenever an error occurs & allows custom error handling.
//If this function returns an error, the error will not be handled & processing will be aborted.
type ErrorHandler func(err error) error

//DefaultErrorHandler does not hadle the error - it just returns the error passed into it.
func DefaultErrorHandler(err error) error {
	return err
}

//RecordReader implementations read records into slices of strings
type RecordReader interface {
	Read(data []byte) (values []string, err error)
}

//Record objects contain all of the Fields which form a record & any child Records.
type Record struct {
	Name     string
	Fields   []Field
	Parent   *Record
	Children []*Record
	//isWithinSplit is set if this record has name SplitOnRecordName
	//or if it is a child of such a record.
	isWithinSplit bool
}

//FindRecord returns a pointer to the record with a Name matching
//recordName and, if present all Fields in matches with the same name,
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
