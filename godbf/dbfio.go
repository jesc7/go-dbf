package godbf

import (
	"encoding/csv"
	"io"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/axgle/mahonia"
)

//NewFromFile create in-memory dbf from file on disk
func NewFromFile(fileName string, codepage string) (table *DbfTable, err error) {
	if s, err := readFile(fileName); err == nil {
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

//NewFromCSVWithSchema create schema-based dbf and fill it from csv file
func NewFromCSVWithSchema(filename string, codepageFrom string, headers bool, skip int, comma rune, schema []DbfSchema, codepageTo string) (table *DbfTable, err error) {
	table, err = NewFromSchema(schema, codepageTo)
	if err != nil {
		return
	}
	f, err := os.Open(filename)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	r := csv.NewReader(mahonia.NewDecoder(codepageFrom).NewReader(f))
	r.LazyQuotes = true
	r.Comma = comma
	var header []string
	for {
		if skip >= 0 {
			r.FieldsPerRecord = 0
		}
		record, err := r.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
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

		recno := table.AddNewRecord()
		for i := range record {
			table.SetFieldValueByName(recno, header[i], record[i])
		}
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
