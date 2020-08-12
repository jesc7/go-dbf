package godbf

import (
	"encoding/csv"
	"os"
	"strings"

	"golang.org/x/text/encoding/charmap"

	"github.com/axgle/mahonia"
)

func NewFromFile(fileName string, fileEncoding string) (table *DbfTable, err error) {
	if s, err := readFile(fileName); err == nil {
		return createDbfTable(s, fileEncoding)
	}
	return
}

func NewFromByteArray(data []byte, fileEncoding string) (table *DbfTable, err error) {
	return createDbfTable(data, fileEncoding)
}

func createDbfTable(s []byte, fileEncoding string) (table *DbfTable, err error) {
	// Create and populate DbaseTable struct
	dt := new(DbfTable)

	dt.fileEncoding = fileEncoding
	dt.encoder = mahonia.NewEncoder(fileEncoding)
	dt.decoder = mahonia.NewDecoder(fileEncoding)

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
			err = dt.AddDateField(fieldName)
		}

		// Check return value for errors
		if err != nil {
			return nil, err
		}

		//fmt.Printf("Field name:%v\n", name)
		//fmt.Printf("Field data type:%v\n", string(s[offset+11]))
		//fmt.Printf("Field fixedFieldLength:%v\n", s[offset+16])
		//fmt.Println("-----------------------------------------------")
	}

	//fmt.Printf("DbfReader:\n%#v\n", dt)
	//fmt.Printf("DbfReader:\n%#v\n", int(dt.Fields[2].fixedFieldLength))

	//fmt.Printf("num records in table:%v\n", (dt.numberOfRecords))
	//fmt.Printf("fixedFieldLength of each record:%v\n", (dt.lengthOfEachRecord))

	// Since we are reading dbase file from the disk at least at this
	// phase changing schema of dbase file is not allowed.
	dt.dataEntryStarted = true

	// set DbfTable dataStore slice that will store the complete file in memory
	dt.dataStore = s

	return dt, nil
}

func (dt *DbfTable) SaveFile(filename string) (err error) {

	f, err := os.Create(filename)

	if err != nil {
		return err
	}

	defer f.Close()

	_, dsErr := f.Write(dt.dataStore)

	if dsErr != nil {
		return dsErr
	}

	// Add dbase end of file marker (1Ah)

	_, footerErr := f.Write([]byte{0x1A})

	if footerErr != nil {
		return footerErr
	}

	return
}

//SaveCSV translate dbf to csv format
func (dt *DbfTable) SaveCSV(filename string, delimiter rune, headers bool) (err error) {
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

	encoder := charmap.Windows1251.NewEncoder()
	w := csv.NewWriter(encoder.Writer(f))
	w.Comma = delimiter
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
		if err := w.Write(row); err != nil {
			return err
		}
		w.Flush()
	}
	return
}
