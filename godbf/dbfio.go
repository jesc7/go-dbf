package godbf

import (
	"encoding/csv"
	"encoding/xml"
	"errors"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/axgle/mahonia"
	"github.com/sergeilem/xls"
	"golang.org/x/net/html/charset"
)

//NewFromFile create in-memory dbf from file on disk
func NewFromFile(fileName string, codepage string) (table *DbfTable, err error) {
	s, err := readFile(fileName)
	if err == nil {
		return createDbfTable(s, codepage)
	}
	return
}

//NewFromByteArray create dbf from byte array
func NewFromByteArray(data []byte, codepage string) (table *DbfTable, err error) {
	return createDbfTable(data, codepage)
}

//NewFromSchema create schema-based dbf
func NewFromSchema(schema []DbfSchema, codepage string) (table *DbfTable, err error) {
	table = New(codepage)
	err = table.AddSchema(schema)
	return
}

//NewFromDBF recreate dbf, aliases and field restrictions are supperted
func NewFromDBF(filename string, codepageFrom string, schema []DbfSchema, codepageTo string) (table *DbfTable, err error) {
	table, err = NewFromSchema(schema, codepageTo)
	if err != nil {
		return
	}
	src, err := NewFromFile(filename, codepageFrom)
	if err != nil {
		return nil, err
	}

	aliases := make(map[string]string)
	for _, v := range src.fields {
		aliases[v.name] = v.name
	}
	for _, v := range schema {
		if len(v.Alias) != 0 {
			if _, found := aliases[v.Alias]; found {
				aliases[v.Alias] = v.FieldName
			}
		}
	}
	var found bool
	for _, v := range aliases {
		if found = table.HasField(v); found {
			break
		}
	}
	if !found {
		return nil, fmt.Errorf("No destination field in schema, will be empty result")
	}
	for i := 0; i < int(src.numberOfRecords); i++ {
		recno := table.AddNewRecord()
		for _, v := range src.fields {
			v2 := aliases[v.name]
			if j, _ := table.FieldIdx(v2); j != -1 {
				value, _ := src.FieldValueByName(recno, v.name)
				value = formatValue(table.fields[j], value)
				table.SetFieldValue(recno, j, value)
			}
		}
	}
	return table, nil
}

//NewFromCSV create schema-based dbf and fill it from csv file
func NewFromCSV(filename string, codepageFrom string, headers bool, skip int, comma rune, schema []DbfSchema, codepageTo string) (table *DbfTable, err error) {
	table, err = NewFromSchema(schema, codepageTo)
	if err != nil {
		return
	}
	f, err := os.Open(filename)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	aliases := make(map[string]string)
	r := csv.NewReader(mahonia.NewDecoder(codepageFrom).NewReader(f))
	r.LazyQuotes = true
	r.Comma = comma
	var (
		header      []string
		fillAliases bool
	)
	for {
		if skip >= 0 {
			r.FieldsPerRecord = 0
		}
		record, err := r.Read()
		if err == io.EOF {
			break
		}
		if err != nil && len(record) < len(header) {
			return nil, err
		}
		if skip--; skip >= 0 {
			continue
		}

		if header == nil {
			if headers {
				header = record
				continue
			}
			for i := 0; i <= len(record); i++ {
				header = append(header, "F"+strconv.Itoa(i+1))
			}
		}
		if !fillAliases {
			for _, v := range header {
				aliases[v] = v
			}
			for _, v := range schema {
				if len(v.Alias) != 0 {
					if _, found := aliases[v.Alias]; found {
						aliases[v.Alias] = v.FieldName
					}
				}
			}
			var found bool
			for _, v := range aliases {
				if found = table.HasField(v); found {
					break
				}
			}
			if !found {
				return nil, fmt.Errorf("No destination field in schema, will be empty result")
			}
			fillAliases = true
		}

		recno := table.AddNewRecord()
		for i := range record {
			if i < len(header) {
				j, err := table.FieldIdx(aliases[header[i]])
				if err != nil {
					continue
				}
				value := formatValue(table.fields[j], record[i])
				table.SetFieldValue(recno, j, value)
			}
		}
	}
	return table, nil
}

//NewFromXLS create schema-based dbf from excel file
func NewFromXLS(filename string, codepageFrom string, sheet string, keycolumn, skip int, schema []DbfSchema, codepageTo string) (table *DbfTable, err error) {
	table, err = NewFromSchema(schema, codepageTo)
	if err != nil {
		return
	}
	xl, err := xls.Open(filename, codepageFrom)
	if err != nil {
		return nil, err
	}

	for i := 0; i < xl.NumSheets(); i++ {
		sh := xl.GetSheet(i)
		if strings.EqualFold(sh.Name, sheet) {
			aliases := make(map[string]string)
			var header []string
			for j := 1 + skip; j <= int(sh.MaxRow); j++ {
				r := sh.Row(j)
				if _, err := strconv.ParseFloat(r.Col(keycolumn), 64); err != nil {
					continue
				}
				record := make([]string, r.FirstCol())
				for k := r.FirstCol(); k <= r.LastCol(); k++ {
					record = append(record, r.Col(k))
				}

				if header == nil {
					for k := range record {
						header = append(header, "F"+strconv.Itoa(k+1))
					}
					for _, v := range header {
						aliases[v] = v
					}
					for _, v := range schema {
						if len(v.Alias) != 0 {
							if _, found := aliases[v.Alias]; found {
								aliases[v.Alias] = v.FieldName
							}
						}
					}
					var found bool
					for _, v := range aliases {
						if found = table.HasField(v); found {
							break
						}
					}
					if !found {
						return nil, fmt.Errorf("No destination field in schema, will be empty result")
					}
				}

				recno := table.AddNewRecord()
				for k := range record {
					if k < len(header) {
						l, err := table.FieldIdx(aliases[header[k]])
						if err != nil {
							continue
						}
						value := formatValue(table.fields[l], record[k])
						table.SetFieldValue(recno, l, value)
					}
				}
			}
			return table, nil
		}
	}
	return nil, fmt.Errorf("No sheet named %s", sheet)
}

//NewFromXML create dbf from XML
func NewFromXML(filename string, codepageFrom string, schema []DbfSchema, codepageTo string) (table *DbfTable, err error) {
	table, err = NewFromSchema(schema, codepageTo)
	if err != nil {
		return
	}
	f, err := os.Open(filename)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	r := xml.NewDecoder(f)
	r.CharsetReader = charset.NewReaderLabel
	aliases := make(map[string]DbfSchema)
	for _, v := range schema {
		if len(v.Alias) != 0 {
			aliases[strings.ToLower(v.Alias)] = v
		}
	}
	var (
		curtag   string
		errFound bool
		errText  string
		recno    int = -1
	)

	utf2x := func(s, codepage string) string {
		return mahonia.NewEncoder(codepage).ConvertString(s)
	}
	addNew := func() (rec int, err error) {
		if errFound {
			if len(errText) > 0 {
				return -1, errors.New(utf2x(errText, codepageFrom))
			}
			return -1, errors.New("Unknown error")
		}
		return table.AddNewRecord(), nil
	}

	for {
		token, _ := r.Token()
		if token == nil {
			break
		}

		switch t := token.(type) {
		case xml.StartElement:
			curtag = strings.ToLower(t.Name.Local)
			if v, found := aliases[curtag+":"]; found && v.FieldName == "__NEW" {
				if recno, err = addNew(); err != nil {
					return nil, err
				}
				for _, a := range t.Attr {
					if v, found = aliases[curtag+":"+a.Name.Local]; found && v.FieldName != "__NEW" && v.FieldName != "" && a.Value != "\n\t" {
						l, _ := table.FieldIdx(v.FieldName)
						value := formatValue(table.fields[l], a.Value)
						table.SetFieldValue(recno, l, value)
					}
				}
			} else if v, found := aliases[curtag]; found && v.FieldName == "__NEW" {
				if recno, err = addNew(); err != nil {
					return nil, err
				}
			}
		case xml.CharData:
			if curtag != "" {
				if v, found := aliases[curtag]; found {
					switch {
					case v.FieldName == "__ERR" && v.Default == string(t):
						errFound = true
					case v.FieldName == "__ERRTEXT":
						return nil, errors.New(utf2x(string(t), codepageFrom))
					case v.FieldName != "" && v.FieldName != "__NEW" && string(t) != "\n\t":
						l, _ := table.FieldIdx(v.FieldName)
						value := formatValue(table.fields[l], string(t))
						table.SetFieldValue(recno, l, value)
					}
				}
				curtag = ""
			}
		}
	}
	if errFound {
		return nil, errors.New("Unknown error")
	}
	return table, nil
}

func createDbfTable(s []byte, codepage string) (table *DbfTable, err error) {
	// Create and populate DbaseTable struct
	dt := new(DbfTable)

	dt.fileEncoding = codepage
	dt.encoder = mahonia.NewEncoder(codepage)
	dt.decoder = mahonia.NewDecoder(codepage)

	// read dbase table header information
	dt.fileSignature = s[0]
	dt.updateYear = s[1]
	dt.updateMonth = s[2]
	dt.updateDay = s[3]
	dt.numberOfRecords = uint32(s[4]) | (uint32(s[5]) << 8) | (uint32(s[6]) << 16) | (uint32(s[7]) << 24)
	dt.numberOfBytesInHeader = uint16(s[8]) | (uint16(s[9]) << 8)
	dt.lengthOfEachRecord = uint16(s[10]) | (uint16(s[11]) << 8)

	// create fieldMap to translate field name to index
	dt.fieldMap = make(map[string]int)

	// Number of fields in dbase table
	dt.numberOfFields = int((dt.numberOfBytesInHeader - 1 - 32) / 32)

	// populate dbf fields
	for i := 0; i < int(dt.numberOfFields); i++ {
		offset := (i * 32) + 32

		fieldName := strings.Trim(dt.encoder.ConvertString(string(s[offset:offset+10])), string([]byte{0}))
		dt.fieldMap[fieldName] = i

		var err error

		switch s[offset+11] {
		case 'C':
			err = dt.AddTextField(fieldName, s[offset+16])
		case 'N':
			err = dt.AddNumberField(fieldName, s[offset+16], s[offset+17])
		case 'F':
			err = dt.AddFloatField(fieldName, s[offset+16], s[offset+17])
		case 'L':
			err = dt.AddBooleanField(fieldName)
		case 'D':
			err = dt.AddDateField(fieldName, "")
		}

		// Check return value for errors
		if err != nil {
			return nil, err
		}
	}

	// Since we are reading dbase file from the disk at least at this
	// phase changing schema of dbase file is not allowed.
	dt.dataEntryStarted = true

	// set DbfTable dataStore slice that will store the complete file in memory
	dt.dataStore = s

	return dt, nil
}

//SaveFile save file on disk
func (dt *DbfTable) SaveFile(filename string) (err error) {
	f, err := os.Create(filename)
	if err != nil {
		return err
	}
	defer f.Close()

	if _, dsErr := f.Write(dt.dataStore); dsErr != nil {
		return dsErr
	}

	// Add dbase end of file marker (1Ah)
	if _, footerErr := f.Write([]byte{0x1A}); footerErr != nil {
		return footerErr
	}
	return
}

//SaveCSV translate dbf to csv format
func (dt *DbfTable) SaveCSV(filename string, codepage string, comma rune, headers bool) (err error) {
	f, err := os.Create(filename)
	if err != nil {
		return err
	}
	defer func() {
		f.Close()
		if err != nil {
			os.Remove(filename)
		}
	}()

	w := csv.NewWriter(mahonia.NewEncoder(codepage).NewWriter(f))
	w.Comma = comma
	if headers {
		fields := dt.Fields()
		fieldRow := make([]string, len(fields))
		for i := 0; i < len(fields); i++ {
			fieldRow[i] = fields[i].Name()
		}
		if err := w.Write(fieldRow); err != nil {
			return err
		}
		w.Flush()
	}

	for i := 0; i < dt.NumberOfRecords(); i++ {
		row := dt.GetRowAsSlice(i)
		for j := range row {
			switch string(dt.fields[j].fieldType) + "_" + dt.fields[j].format {
			case "D_RFC3339":
				t, _ := time.Parse("20060102", row[j])
				row[j] = t.Format(time.RFC3339)
			case "D_02.01.2006":
				t, _ := time.Parse("20060102", row[j])
				row[j] = t.Format("02.01.2006")
			}
		}

		if err := w.Write(row); err != nil {
			return err
		}
		w.Flush()
	}
	return
}
