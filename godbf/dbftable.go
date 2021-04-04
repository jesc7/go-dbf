package godbf

import (
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/axgle/mahonia"
)

const (
	null    = 0x00
	blank   = 0x20
	deleted = 0x2A
)

//DbfTable describe dbf file
type DbfTable struct {
	// dbase file header information
	fileSignature         uint8 // Valid dBASE III PLUS table file (03h without a memo .DBT file; 83h with a memo)
	updateYear            uint8 // Date of last update; in YYMMDD format.
	updateMonth           uint8
	updateDay             uint8
	numberOfRecords       uint32   // Number of records in the table.
	numberOfBytesInHeader uint16   // Number of bytes in the header.
	lengthOfEachRecord    uint16   // Number of bytes in the record.
	reservedBytes         [20]byte // Reserved bytes
	fieldDescriptor       [32]byte // Field descriptor array
	fieldTerminator       int8     // 0Dh stored as the field terminator.

	numberOfFields int // number of fiels/colums in dbase file

	// columns of dbase file
	fields []FieldDescriptor

	// used to map field names to index
	fieldMap map[string]int
	/*
	   "dataEntryStarted" flag is used to control whether we can change
	   dbase table structure when data enty started you can not change
	   the schema of the file if you are reading from an existing file this
	   file will be set to "true". This means you can not modify the schema
	   of a dbase table that you loaded from a file.
	*/
	dataEntryStarted bool

	// cratedFromScratch is used before adding new fields to increment nu
	createdFromScratch bool

	// encoding of dbase file
	fileEncoding string
	decoder      mahonia.Decoder
	encoder      mahonia.Encoder

	// keeps the dbase table in memory as byte array
	dataStore []byte
}

//DbfSchema describe table fields
type DbfSchema struct {
	FieldName     string `json:"name"`
	Alias         string `json:"alias"`
	Header        bool   `json:"header"`
	DataType      string `json:"type"`
	FieldLength   byte   `json:"length"`
	DecimalPlaces uint8  `json:"decimal"`
	Format        string `json:"format"`
	Default       string `json:"default"`
	Expr          string `json:"expr"`
}

// New creates a new dbase table from scratch for the given character encoding
func New(encoding string) (table *DbfTable) {

	// Create and populate DbaseTable struct
	dt := new(DbfTable)

	dt.fileEncoding = encoding
	dt.encoder = mahonia.NewEncoder(encoding)
	dt.decoder = mahonia.NewDecoder(encoding)

	// set whether or not this table has been created from scratch
	dt.createdFromScratch = true

	// read dbase table header information
	dt.fileSignature = 0x03
	dt.updateYear = byte(time.Now().Year() - 1900)
	dt.updateMonth = byte(time.Now().Month())
	dt.updateDay = byte(time.Now().Day())
	dt.numberOfRecords = 0
	dt.numberOfBytesInHeader = 32
	dt.lengthOfEachRecord = 0

	// create fieldMap to translate field name to index
	dt.fieldMap = make(map[string]int)

	// Number of fields in dbase table
	dt.numberOfFields = int((dt.numberOfBytesInHeader - 1 - 32) / 32)

	s := make([]byte, dt.numberOfBytesInHeader)

	//fmt.Printf("number of fields:\n%#v\n", numberOfFields)
	//fmt.Printf("DbfReader:\n%#v\n", int(dt.Fields[2].fixedFieldLength))

	//fmt.Printf("num records in table:%v\n", (dt.numberOfRecords))
	//fmt.Printf("fixedFieldLength of each record:%v\n", (dt.lengthOfEachRecord))

	// Since we are reading dbase file from the disk at least at this
	// phase changing schema of dbase file is not allowed.
	dt.dataEntryStarted = false

	// set DbfTable dataStore slice that will store the complete file in memory
	dt.dataStore = s

	dt.dataStore[0] = dt.fileSignature
	dt.dataStore[1] = dt.updateYear
	dt.dataStore[2] = dt.updateMonth
	dt.dataStore[3] = dt.updateDay

	// no MDX file (index upon demand)
	dt.dataStore[28] = 0x00

	// set dbase language driver
	// Huston we have problem!
	// There is no easy way to deal with encoding issues. At least at the moment
	// I will try to find archaic encoding code defined by dbase standard (if there is any)
	// for given encoding. If none match I will go with default ANSI.
	//
	// Despite this flag in set in dbase file, I will continue to use provide encoding for
	// the everything except this file encoding flag.
	//
	// Why? To make sure at least if you know the real encoding you can process text accordingly.

	if code, ok := encodingTable[lookup[encoding]]; ok {
		dt.dataStore[29] = code
	} else {
		dt.dataStore[29] = 0x57 // ANSI
	}

	return dt
}

//AddSchema add fields to new dbf
func (dt *DbfTable) AddSchema(sch []DbfSchema) (err error) {
	for _, f := range sch {
		if len(f.DataType) != 0 {
			switch DbaseDataType(f.DataType[0]) {
			case Character:
				err = dt.AddTextField(f.FieldName, f.FieldLength)
			case Date:
				err = dt.AddDateField(f.FieldName, f.Format)
			case Float:
				err = dt.AddFloatField(f.FieldName, f.FieldLength, f.DecimalPlaces)
			case Logical:
				err = dt.AddBooleanField(f.FieldName)
			case Numeric:
				err = dt.AddNumberField(f.FieldName, f.FieldLength, f.DecimalPlaces)
			}
			if err != nil {
				return err
			}
		}
	}
	return nil
}

//AddFieldAs add field to dst like another from src
func (dt *DbfTable) AddFieldAs(src *FieldDescriptor, name string) (err error) {
	switch src.FieldType() {
	case Character:
		err = dt.AddTextField(name, src.Length())
	case Date:
		err = dt.AddDateField(name, src.Format())
	case Float:
		err = dt.AddFloatField(name, src.Length(), src.DecimalCount())
	case Logical:
		err = dt.AddBooleanField(name)
	case Numeric:
		err = dt.AddNumberField(name, src.Length(), src.DecimalCount())
	}
	if err != nil {
		return err
	}
	return nil
}

func (dt *DbfTable) AddBooleanField(fieldName string) (err error) {
	return dt.addField(fieldName, Logical, Logical.fixedFieldLength(), Logical.decimalCountNotApplicable(), "")
}

func (dt *DbfTable) AddDateField(fieldName string, format string) (err error) {
	return dt.addField(fieldName, Date, Date.fixedFieldLength(), Date.decimalCountNotApplicable(), format)
}

func (dt *DbfTable) AddTextField(fieldName string, length byte) (err error) {
	return dt.addField(fieldName, Character, length, Character.decimalCountNotApplicable(), "")
}

func (dt *DbfTable) AddNumberField(fieldName string, length byte, decimalPlaces uint8) (err error) {
	return dt.addField(fieldName, Numeric, length, decimalPlaces, "")
}

func (dt *DbfTable) AddFloatField(fieldName string, length byte, decimalPlaces uint8) (err error) {
	return dt.addField(fieldName, Float, length, decimalPlaces, "")
}

func (dt *DbfTable) addField(fieldName string, fieldType DbaseDataType, length byte, decimalPlaces uint8, format string) (err error) {

	if dt.dataEntryStarted {
		return errors.New("Once you start entering data to the dbase table or open an existing dbase file, altering dbase table schema is not allowed!")
	}

	normalizedFieldName := dt.normaliseFieldName(fieldName)

	if dt.HasField(normalizedFieldName) {
		return errors.New("Field name \"" + normalizedFieldName + "\" already exists!")
	}

	df := new(FieldDescriptor)
	df.name = normalizedFieldName
	df.fieldType = fieldType
	df.length = length
	df.decimalPlaces = decimalPlaces
	df.format = format

	slice := dt.convertToByteSlice(df.name, 10)

	//fmt.Printf("len slice:%v\n", len(slice))

	// Field name in ASCII (max 10 chracters)
	for i := 0; i < len(slice); i++ {
		df.fieldStore[i] = slice[i]
		//fmt.Printf("i:%s\n", string(slice[i]))
	}

	// Field names are terminated by 00h
	df.fieldStore[10] = 0x00

	// Set field's data type
	// C (Character)  All OEM code page characters.
	// D (Date)     Numbers and a character to separate month, day, and year (stored internally as 8 digits in YYYYMMDD format).
	// N (Numeric)    - . 0 1 2 3 4 5 6 7 8 9
	// F (Floating Point)   - . 0 1 2 3 4 5 6 7 8 9
	// L (Logical)    ? Y y N n T t F f (? when not initialized).
	df.fieldStore[11] = df.fieldType.byte()

	// fixedFieldLength of field
	df.fieldStore[16] = df.length

	// number of decimal places
	// Applicable only to number/float
	df.fieldStore[17] = df.decimalPlaces

	//fmt.Printf("addField | append:%v\n", df)

	dt.fields = append(dt.fields, *df)

	// if createdFromScratch we need to update dbase header to reflect the changes we have made
	if dt.createdFromScratch {
		dt.updateHeader()
	}

	return
}

func (dt *DbfTable) normaliseFieldName(name string) (s string) {
	e := mahonia.NewEncoder(dt.fileEncoding)
	b := []byte(e.ConvertString(name))

	if len(b) > 10 {
		b = b[0:10]
	}

	d := mahonia.NewDecoder(dt.fileEncoding)
	s = d.ConvertString(string(b))

	return
}

/*
  getByteSlice converts value to byte slice according to given encoding and return
  a slice that is fixedFieldLength equals to numberOfBytes or less if the string is shorter than
  numberOfBytes
*/
func (dt *DbfTable) convertToByteSlice(value string, numberOfBytes int) (s []byte) {
	e := mahonia.NewEncoder(dt.fileEncoding)
	b := []byte(e.ConvertString(value))

	if len(b) <= numberOfBytes {
		s = b
	} else {
		s = b[0:numberOfBytes]
	}
	return
}

func (dt *DbfTable) updateHeader() {
	// first create a slice from initial 32 bytes of datastore as the foundation of the new slice
	// later we will set this slice to dt.dataStore to create the new header slice
	slice := dt.dataStore[0:32]

	// set dbase file signature
	slice[0] = 0x03

	var lengthOfEachRecord uint16 = 0

	for i := range dt.Fields() {
		lengthOfEachRecord += uint16(dt.Fields()[i].length)
		slice = appendSlice(slice, dt.Fields()[i].fieldStore[:])

		// don't forget to update fieldMap. We need it to find the index of a field name
		dt.fieldMap[dt.Fields()[i].name] = i
	}

	// end of file header terminator (0Dh)
	slice = appendSlice(slice, []byte{0x0D})

	// now reset dt.dataStore slice with the updated one
	dt.dataStore = slice

	// update the number of bytes in dbase file header
	dt.numberOfBytesInHeader = uint16(len(slice))
	s := uint32ToBytes(uint32(dt.numberOfBytesInHeader))
	dt.dataStore[8] = s[0]
	dt.dataStore[9] = s[1]

	dt.lengthOfEachRecord = lengthOfEachRecord + 1 // dont forget to add "1" for deletion marker which is 20h

	// update the lenght of each record
	s = uint32ToBytes(uint32(dt.lengthOfEachRecord))
	dt.dataStore[10] = s[0]
	dt.dataStore[11] = s[1]

	return
}

// Fields return the fields of the table as a slice
func (dt *DbfTable) Fields() []FieldDescriptor {
	return dt.fields
}

// FieldByName return the field descriptor of the table
func (dt *DbfTable) FieldByName(fieldName string) FieldDescriptor {

	for i := 0; i < len(dt.fields); i++ {
		if dt.fields[i].name == fieldName {
			return dt.fields[i]
		}
	}

	return FieldDescriptor{}
}

// FieldNames return the names of fields in the table as a slice
func (dt *DbfTable) FieldNames() []string {
	names := make([]string, 0)

	for _, field := range dt.Fields() {
		names = append(names, field.name)
	}

	return names
}

// HasField returns true if the table has a field with the given name
// If the field does not exist an error is returned.
func (dt *DbfTable) HasField(fieldName string) bool {

	for i := 0; i < len(dt.fields); i++ {
		if dt.fields[i].name == fieldName {
			return true
		}
	}

	return false
}

// FieldIdx returns field index if the table has a field with the given name
// If the field does not exist an error is returned.
func (dt *DbfTable) FieldIdx(fieldName string) (int, error) {
	for i := 0; i < len(dt.fields); i++ {
		if dt.fields[i].name == fieldName {
			return i, nil
		}
	}
	return -1, errors.New("Field name \"" + fieldName + "\" not exists")
}

// DecimalPlacesInField returns the number of decimal places for the field with the given name.
// If the field does not exist, or does not use decimal places, an error is returned.
func (dt *DbfTable) DecimalPlacesInField(fieldName string) (uint8, error) {
	if !dt.HasField(fieldName) {
		return 0, errors.New("Field name \"" + fieldName + "\" does not exist. ")
	}

	for i := 0; i < len(dt.fields); i++ {
		if dt.fields[i].name == fieldName && dt.fields[i].usesDecimalPlaces() {
			return dt.fields[i].decimalPlaces, nil
		}
	}

	return 0, errors.New("Type of field \"" + fieldName + "\" is not Numeric or Float.")
}

// AddNewRecord adds a new empty record to the table, and returns the index number of the record.
func (dt *DbfTable) AddNewRecord() (newRecordNumber int) {

	if dt.dataEntryStarted == false {
		dt.dataEntryStarted = true
	}

	newRecord := make([]byte, dt.lengthOfEachRecord)
	newRecord[0] = blank
	dt.dataStore = appendSlice(dt.dataStore, newRecord)

	// since row numbers are "0" based first we set newRecordNumber
	// and then increment number of records in dbase table
	newRecordNumber = int(dt.numberOfRecords)

	//fmt.Printf("Number of rows before:%d\n", dt.numberOfRecords)
	dt.numberOfRecords++
	s := uint32ToBytes(dt.numberOfRecords)
	dt.dataStore[4] = s[0]
	dt.dataStore[5] = s[1]
	dt.dataStore[6] = s[2]
	dt.dataStore[7] = s[3]
	//fmt.Printf("Number of rows after:%d\n", dt.numberOfRecords)

	return newRecordNumber
}

// NumberOfRecords returns the number of records in the table
func (dt *DbfTable) NumberOfRecords() int {
	return int(dt.numberOfRecords)
}

// SetFieldValueByName sets the value for the given row and field name as specified
// If the field name does not exist, or the value is incompatible with the field's type, an error is returned.
func (dt *DbfTable) SetFieldValueByName(row int, fieldName string, value string) (err error) {
	if fieldIndex, found := dt.fieldMap[fieldName]; found {
		return dt.SetFieldValue(row, fieldIndex, value)
	}
	return errors.New("Field name \"" + fieldName + "\" does not exist")
}

// SetFieldValue sets the value for the given row and field index as specified
// If the field index is invalid, or the value is incompatible with the field's type, an error is returned.
func (dt *DbfTable) SetFieldValue(row int, fieldIndex int, value string) (err error) {
	b := []byte(dt.encoder.ConvertString(value))
	fieldLength := int(dt.fields[fieldIndex].length)

	// locate the offset of the field in DbfTable dataStore
	offset := int(dt.numberOfBytesInHeader)
	lengthOfRecord := int(dt.lengthOfEachRecord)
	offset = offset + (row * lengthOfRecord)
	recordOffset := 1

	for i := 0; i < len(dt.fields); i++ {
		if i == fieldIndex {
			break
		} else {
			recordOffset += int(dt.fields[i].length)
		}
	}

	dt.fillFieldWithBlanks(fieldLength, offset, recordOffset)

	// write new value
	switch dt.fields[fieldIndex].fieldType {
	case Character, Logical, Date:
		for i := 0; i < len(b) && i < fieldLength; i++ {
			dt.dataStore[offset+recordOffset+i] = b[i]
		}
	case Float, Numeric:
		for i := 0; i < fieldLength; i++ {
			if i < len(b) {
				dt.dataStore[offset+recordOffset+(fieldLength-i-1)] = b[(len(b)-1)-i]
			} else {
				break
			}
		}
	}
	return
}

func (dt *DbfTable) fillFieldWithBlanks(fieldLength int, offset int, recordOffset int) {
	for i := 0; i < fieldLength; i++ {
		dt.dataStore[offset+recordOffset+i] = blank
	}
}

//FieldValue returns the content for the record at the given row and field index as a string
// If the row or field index is invalid, an error is returned .
func (dt *DbfTable) FieldValue(row int, fieldIndex int) (value string) {

	offset := int(dt.numberOfBytesInHeader)
	lengthOfRecord := int(dt.lengthOfEachRecord)

	offset = offset + (row * lengthOfRecord)

	recordOffset := 1

	for i := 0; i < len(dt.fields); i++ {
		if i == fieldIndex {
			break
		} else {
			recordOffset += int(dt.fields[i].length)
		}
	}

	temp := dt.dataStore[(offset + recordOffset):((offset + recordOffset) + int(dt.fields[fieldIndex].length))]

	enforceBlankPadding(temp)

	s := dt.decoder.ConvertString(string(temp))
	//fmt.Printf("utf-8 value:[%#v] original value:[%#v]\n", s, string(temp))

	value = strings.TrimSpace(s)

	//fmt.Printf("raw value:[%#v]\n", dt.dataStore[(offset + recordOffset):((offset + recordOffset) + int(dt.Fields[fieldIndex].fixedFieldLength))])
	//fmt.Printf("utf-8 value:[%#v]\n", []byte(s))
	//value = string(dt.dataStore[(offset + recordOffset):((offset + recordOffset) + int(dt.Fields[fieldIndex].fixedFieldLength))])
	return
}

// Some Dbf encoders pad with null chars instead of blanks, this forces blanks as per
// https://www.dbase.com/Knowledgebase/INT/db7_file_fmt.htm
func enforceBlankPadding(temp []byte) {
	for i := 0; i < len(temp); i++ {
		if temp[i] == null {
			temp[i] = blank
		}
	}
}

// Float64FieldValueByName returns the value of a field given row number and name provided as a float64
func (dt *DbfTable) Float64FieldValueByName(row int, fieldName string) (value float64, err error) {
	valueAsString, err := dt.FieldValueByName(row, fieldName)
	return strconv.ParseFloat(valueAsString, 64)
}

// Int64FieldValueByName returns the value of a field given row number and name provided as an int64
func (dt *DbfTable) Int64FieldValueByName(row int, fieldName string) (value int64, err error) {
	valueAsString, err := dt.FieldValueByName(row, fieldName)
	return strconv.ParseInt(valueAsString, 0, 64)
}

// FieldValueByName returns the value of a field given row number and name provided
func (dt *DbfTable) FieldValueByName(row int, fieldName string) (value string, err error) {
	if fieldIndex, entryFound := dt.fieldMap[fieldName]; entryFound {
		return dt.FieldValue(row, fieldIndex), err
	}
	err = errors.New("Field name \"" + fieldName + "\" does not exist")
	return
}

//RowIsDeleted returns whether a row has marked as deleted
func (dt *DbfTable) RowIsDeleted(row int) bool {
	if row < 0 || row >= int(dt.NumberOfRecords()) {
		return false
	}
	offset := int(dt.numberOfBytesInHeader)
	lengthOfRecord := int(dt.lengthOfEachRecord)
	offset = offset + (row * lengthOfRecord)
	return dt.dataStore[offset:(offset + 1)][0] == deleted
}

//DeleteRow deleted row by num
func (dt *DbfTable) DeleteRow(row int) error {
	if row < 0 || row >= int(dt.NumberOfRecords()) {
		return errors.New("Out of range")
	}
	offset := int(dt.numberOfBytesInHeader) + (row * int(dt.lengthOfEachRecord))
	dt.dataStore[offset:(offset + 1)][0] = deleted
	return nil
}

// GetRowAsSlice return the record values for the row specified as a string slice
func (dt *DbfTable) GetRowAsSlice(row int) []string {
	s := make([]string, len(dt.Fields()))
	for i := 0; i < len(dt.Fields()); i++ {
		s[i] = dt.FieldValue(row, i)
	}
	return s
}

func formatValue(f FieldDescriptor, value string) string {
	left := func(str string, length int) string {
		runes := []rune(str)
		if len(runes) > length {
			return string(runes[:length])
		}
		return str
	}

	if value != "" {
		switch f.fieldType {
		case 'C':
			value = left(value, int(f.length))
		case 'N':
			if n, err := strconv.ParseFloat(value, 32); err == nil {
				value = fmt.Sprintf("%."+strconv.Itoa(int(f.decimalPlaces))+"f", n)
			}
		case 'D':
			switch f.format {
			case "RFC3339":
				t, _ := time.Parse(time.RFC3339, value)
				value = t.Format("20060102")
			case "02.01.2006":
				t, _ := time.Parse("02.01.2006", value)
				value = t.Format("20060102")
			}
		}
	}
	return value
}
