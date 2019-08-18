package structuredfilereader

import (
	"encoding/json"
	"io/ioutil"
	"os"
	"reflect"
	"strings"
	"testing"
	"time"
)

func TestUnmarshalFixedWidthRecordReader(t *testing.T) {
	//SetLogOutput(os.Stdout)
	configStr := `
    {
      "Name": "TestRecordDefinition01",
			"ReaderName": "FixedWidth",
      "RecordReader": {
        "Coordinates": [
          {
            "Start": 0,
            "End": 6
          },
          {
            "Start": 6,
            "End": 16
          }
        ]
      },
      "JoinOnFieldNames": ["X", "Y"],
      "ParentRecordName": "Z"
    }
  `
	var recDef RecordDefinition
	err := json.Unmarshal([]byte(configStr), &recDef)
	if err != nil {
		t.Error(err)
		return
	}

	json.NewEncoder(os.Stdout).Encode(recDef)

	logger.Printf("RecordReader is a %s\n", reflect.TypeOf(recDef.RecordReader).Name())
}

func TestUnmarshalDelimitedRecordReader(t *testing.T) {
	//SetLogOutput(os.Stdout)
	configStr := `
    {
      "Name": "TestRecordDefinition01",
			"ReaderName": "Delimited",
      "RecordReader": {
        "Delimiter": "~"
      }
    }
  `
	var recDef RecordDefinition
	err := json.Unmarshal([]byte(configStr), &recDef)
	if err != nil {
		t.Error(err)
		return
	}

	json.NewEncoder(os.Stdout).Encode(recDef)

	logger.Printf("RecordReader is a %s\n", reflect.TypeOf(recDef.RecordReader).Name())
}

const stringNumDateCfg = `
    {
      "Name": "TestRecordDefinition01",
			"ReaderName": "Delimited",
      "RecordReader": {
        "Delimiter": "~"
      },
			"FieldDefinitions": [
				{
					"Name": "TestStringField",
					"TypeName": "String"
				},
				{
					"Name": "TestNumberField",
					"TypeName": "Number",
					"FieldType": {
						"ConvertToDecimalPlaces": 3
					}
				},
				{
					"Name": "TestDateField",
					"TypeName": "Date",
					"FieldType": {
						"Format": "20060102150405"
					}
				}

			]
    }
  `

func TestUnmarshalDelimitedFieldDefStringAndNumberAndDate(t *testing.T) {
	//SetLogOutput(os.Stdout)
	var recDef RecordDefinition
	err := json.Unmarshal([]byte(stringNumDateCfg), &recDef)
	if err != nil {
		t.Error(err)
		return
	}

	json.NewEncoder(os.Stdout).Encode(recDef)

	logger.Printf("RecordReader is a %s\n", reflect.TypeOf(recDef.RecordReader).Name())
}

const JoinCfg = `
{
	"RecordDefinitions": [
    {
      "Name": "InvoiceHeader",
			"MatchExpression": "^010",
			"ReaderName": "Delimited",
      "RecordReader": {
        "Delimiter": "~"
      },
			"FieldDefinitions": [
				{
					"Name": "RecordID",
					"TypeName": "String"
				},
				{
					"Name": "InvoiceNumber",
					"TypeName": "String"
				},
				{
					"Name": "InvoiceAmount",
					"TypeName": "Number",
					"FieldType": {
						"ConvertToDecimalPlaces": 2
					}
				},
				{
					"Name": "InvoiceDate",
					"TypeName": "Date",
					"FieldType": {
						"Format": "02-Jan-2006"
					}
				}
			]
    },
		{
			"Name": "InvoiceLine",
			"MatchExpression": "^030",
			"ReaderName": "Delimited",
      "RecordReader": {
        "Delimiter": "~"
      },
			"ParentRecordName": "InvoiceHeader",
			"FieldDefinitions": [
				{
					"Name": "RecordID",
					"TypeName": "String"
				},
				{
					"Name": "LineNumber",
					"TypeName": "String"
				},
				{
					"Name": "Description",
					"TypeName": "String"
				}
			]
		},
		{
			"Name": "InvoiceLineDist",
			"MatchExpression": "^033",
			"ReaderName": "FixedWidth",
      "RecordReader": {
				"Coordinates": [
          {
            "Start": 0,
            "End": 3
          },
          {
            "Start": 3,
            "End": 13
          }
        ]
      },
			"ParentRecordName": "InvoiceLine",
			"FieldDefinitions": [
				{
					"Name": "RecordID",
					"TypeName": "String"
				},
				{
					"Name": "Account",
					"TypeName": "String"
				}
			]
		}
	]
}
`
const HierarchyData = `
010~INV98765~12345~17-JUL-2019
030~0001~Invoice One, Line One
033ACCTNUM001
030~0002~Invoice One, Line Two
033ACCTNUM002
010~INV22222222~12345~17-JUL-2019
030~0001~Invoice Two, Line One
033ACCTNUM221
`

func TestParseJoins(t *testing.T) {
	// SetLogOutput(os.Stdout)
	p, err := NewParser(ioutil.NopCloser(strings.NewReader(JoinCfg)))
	if err != nil {
		t.Error(err)
		return
	}
	recChan, errChan := p.Parse(ioutil.NopCloser(strings.NewReader(HierarchyData)))

	invoices := make([]*Record, 0)
channelListener:
	for {
		select {
		case err := <-errChan:
			if err == nil {
				logger.Println("Received nil Record (on error channel)- exiting.")
				break channelListener
			}
			t.Errorf("Received error: %s", *err)
		case rec := <-recChan:
			if rec == nil {
				logger.Println("Received nil Record (on record channel)- exiting.")
				break channelListener
			}
			//json.NewEncoder(os.Stdout).Encode(rec)
			invoices = append(invoices, rec)
		}
	}

	// V A L I D A T I O N S
	invnum, err := invoices[0].GetField("InvoiceNumber")
	if err != nil {
		t.Error(err)
		return
	}
	if invnum.Value != "INV98765" {
		t.Errorf("Expected INV98765, got %s", invnum.Value)
		return
	}

	invamt, err := invoices[0].GetField("InvoiceAmount")
	if err != nil {
		t.Error(err)
		return
	}
	if invamt.Value != 123.45 {
		t.Errorf("Expected 123.45, got %f", invamt.Value)
		return
	}

	invdate, err := invoices[0].GetField("InvoiceDate")
	if err != nil {
		t.Error(err)
		return
	}
	if invdate.Value != time.Date(2019, 7, 17, 0, 0, 0, 0, time.UTC) {
		t.Errorf("Expected %v, got %v", time.Date(2019, 7, 17, 0, 0, 0, 0, time.UTC), invdate.Value)
		return
	}

	line2 := invoices[0].FindRecord(
		"InvoiceLine",
		[]Field{
			Field{
				Name:     "LineNumber",
				TypeName: "String",
				Value:    "0002",
			},
		},
	)
	line2Desc, err := line2.GetField("Description")
	if err != nil {
		t.Error(err)
		return
	}
	if line2Desc.Value != "Invoice One, Line Two" {
		t.Errorf("Expected \"Invoice One, Line Two\". Got \"%s\"", line2Desc.Value)
		return
	}
}

func TestDelimitedPO(t *testing.T) {
	//SetLogOutput(os.Stdout)
	config, err := os.Open("testfiles/DelimitedPurchaseOrder/po.json")
	if err != nil {
		t.Error(err)
		return
	}
	p, err := NewParser(config)
	if err != nil {
		t.Error(err)
		return
	}

	p.ProcessFile(
		ProcessorFunc(func(record *Record, err error) {
			if err != nil {
				t.Errorf("Callback received error: %s", err)
				return
			}
			jsonBytes, _ := json.MarshalIndent(record, "", "  ")
			logger.Println(string(jsonBytes))
		},
		),
		"testfiles", "DelimitedPurchaseOrder", "po.dat",
	)
}

func TestMissinFile(t *testing.T) {
	p, err := NewParser(ioutil.NopCloser(strings.NewReader(JoinCfg)))
	if err != nil {
		t.Error(err)
		return
	}
	p.ProcessFile(
		ProcessorFunc(func(record *Record, err error) {
			if err == nil {
				t.Error(err)
			}
		}),
		"some", "junk", "file.dat",
	)
}
