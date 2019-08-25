# structuredfilereader
Reads structured flat-files into a Record/Field structure.

Supports both Fixed Width & Delimited (CSV) file formats (including a mixture within the same file).

Supports hierarchical relationships between records in the file.
For example, a file might contain Purchase Order Header, Line & Shipment records.
Regular expressions can be defined to identify the different record types & relationships can be created between those types by defining a "ParentRecordName" on the child record type.
Reads file configuration from a JSON file descriptor (see testfiles/DelimitedPurchaseOrder/po.json for example) or you can create them programmatically.
Records must be ordered in the file so that child records are listed after their parent (a child record will be attached to the last parent with a matching "ParentRecordName" found in the file).

Supports converting field content from the file into Go data types (string, float64, date).

NewParser creates a Parser using:
- Config - an io.ReadCloser which points to a JSON configuration describing the file.
- SplitOnRecordName - the name of a Record described in Config to send to the RecordProcessor. Because a record hierarchy is built, this value is needed to declare the level in the hierarchy that should be passed to the RecordProcessor. The Record passed will be able to access both parent & child Records in the structure.  
- RecordProcessor - a function which will process the Record.
- ErrorHandler - a function to call if an error occurs. If the function returns an error, processing is halted. If it handles the error & returns nil, processing continues.

Parse & ParseFile will process a io.ReadCloser or os.File respectively using the configured Parser.

Example:
```
//Open the config file.
config, err := os.Open("testfiles/DelimitedPurchaseOrder/po.json")
if err != nil {
  t.Error(err)
  return
}

//Create a new Parser from the config file.
p, err := NewParser(
  config,
  //For the RecordProcessor, we'll provide a function that prints a json
  //representation of the Record to stdout.
  func(record *Record) error {
	   return json.NewEncoder(os.Stdout).Encode(record)
  },
  //Providing nil uses the default ErrorHandler which terminates on all errors.
  nil,
)
if err != nil {
  t.Error(err)
  return
}

//Parse the file.
err = p.ParseFile("testfiles", "DelimitedPurchaseOrder", "po.dat")
}
```
