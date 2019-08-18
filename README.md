# structuredfilereader
Reads structured flat-files into a Record/Field structure.

Supports both Fixed Width & Delimited (CSV) file formats (including a mixture within the same file).

Supports hierarchical relationships between records in the file.
For example, a file might contain Purchase Order Header, Line & Shipment records.
Regular expressions can be defined to identify the different record types & relationships can be created between those types by defining a "ParentRecordName" on the child record type.
Reads file configuration from a JSON file descriptor (see testfiles/DelimitedPurchaseOrder/po.json for example).
Records must be ordered in the file so that child records are listed after their parent (a child record will be attached to the last parent with a matching "ParentRecordName" found in the file).

Supports converting field content from the file into Go data types (string, float64, date).

Process & ProcessFile execute the provided callback passing either a pointer to a Record or an error. If an error is passed, processing ends and no more calls to the callback will be made.

Example:
```
//Open the config file.
config, err := os.Open("testfiles/DelimitedPurchaseOrder/po.json")
if err != nil {
  t.Error(err)
  return
}

//Create a new Parser from the config file.
p, err := NewParser(config)
if err != nil {
  t.Error(err)
  return
}

p.ProcessFile(
  ProcessorFunc(func(record *Record, err error) {
    if err != nil {
      fmt.Printf("Callback received error: %s", err)
      return
    }
    //Execute your custom logic here.
    //In this example, we are just fomratting as JSON & printing the Record.
    jsonBytes, _ := json.MarshalIndent(record, "", "  ")
    logger.Println(string(jsonBytes))
  },
  ),
  "testfiles", "DelimitedPurchaseOrder", "po.dat",
)
```

Parse & ParseFile use channels to communicate each "parentless" record (and any children) as they are constructed in order to support a chunked streaming capability so that large files can be processed without consuming large amounts of memory. This is provided in case something more flexible than a callback is required.

Example:
```
//Open the config file.
config, err := os.Open("testfiles/DelimitedPurchaseOrder/po.json")
if err != nil {
  t.Error(err)
  return
}

//Create a new Parser from the config file.
p, err := NewParser(config)
if err != nil {
  t.Error(err)
  return
}

//Parse the file which will write to
recChan, errChan := p.ParseFile("testfiles", "DelimitedPurchaseOrder", "po.dat")

//Optionally configure logging to write logs somewhere (defaults to dev/null).
logger.SetOutput(os.Stdout)

//Loop over a select on the channels. When EOF is reached in the file, the
//channels will be closed so receiving a "nil" on eaither channel means the
//file has been fully processed.
channelListener:
for {
  select {
  case err := <-errChan:
    if err == nil {
      fmt.Println("Received nil Record (on error channel)- exiting.")
      break channelListener
    }
    t.Errorf("Received error: %s", *err)
  case rec := <-recChan:
    if rec == nil {
      fmt.Println("Received nil Record (on record channel)- exiting.")
      break channelListener
    }

    //Do whatever you want to do as the Record objects are received.
    //If you don't want a chunked-streaming capability, you could simply add
    //the Records to a slice. Here we're just printing them in JSON.
    jsonBytes, _ := json.MarshalIndent(rec, "", "  ")
    logger.Println(string(jsonBytes))
  }
}
```
