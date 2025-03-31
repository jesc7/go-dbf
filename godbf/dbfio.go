package godbf

import (
	"bytes"
	"context"
	"encoding/csv"
	"encoding/json"
	"encoding/xml"
	"errors"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/antonmedv/expr"
	"github.com/axgle/mahonia"
	"github.com/sergeilem/xls"
	"golang.org/x/net/html/charset"
)

type errorEmpty struct{}

func (m *errorEmpty) Error() string {
	return "No destination field in schema, will be empty result"
}

// NewFromFile create in-memory dbf from file on disk
func NewFromFile(fileName string, codepage string) (table *DbfTable, err error) {
	s, err := readFile(fileName)
	if err == nil {
		if len(s) == 0 {
			return nil, errors.New("file is empty")
		}
		return createDbfTable(s, codepage)
	}
	return
}

// NewFromByteArray create dbf from byte array
func NewFromByteArray(data []byte, codepage string) (table *DbfTable, err error) {
	if len(data) == 0 {
		return nil, errors.New("empty data")
	}
	return createDbfTable(data, codepage)
}

func SaveToFile(data []byte, filename string) (err error) {
	f, err := os.Create(filename)
	if err != nil {
		return err
	}
	defer f.Close()

	if _, dsErr := f.Write(data); dsErr != nil {
		return dsErr
	}

	// Add dbase end of file marker (1Ah)
	if _, footerErr := f.Write([]byte{0x1A}); footerErr != nil {
		return footerErr
	}
	return
}

func JoinSchemas(base, detail []DbfSchema) (res []DbfSchema) {
	res = append(res, base...)
	m := make(map[string]int, len(res))
	for i := range res {
		m[res[i].FieldName] = i
	}
	for _, v := range detail {
		if i, ok := m[v.FieldName]; ok {
			res[i].Alias = v.Alias
			res[i].Header = v.Header
			res[i].Format = v.Format
			res[i].Expr = v.Expr
			if res[i].FieldName[:2] == "__" {
				res[i].Default = v.Default
			}
		}
	}
	return
}

// NewFromSchema create schema-based dbf
func NewFromSchema(schema []DbfSchema, codepage string) (DbfTable, error) {
	table := *New(codepage)
	e := table.AddSchema(schema)
	return table, e
}

// NewFromDBF recreate dbf, aliases and field restrictions are supported
func NewFromDBF(ctx context.Context, filename string, codepageFrom string, schema []DbfSchema, codepageTo string) (DbfTable, error) {
	f, e := os.Open(filename)
	if e != nil {
		return DbfTable{}, e
	}
	defer f.Close()
	return NewFromDBFReader(ctx, f, codepageFrom, schema, codepageTo)
}

func NewFromDBFReader(ctx context.Context, src io.Reader, codepageFrom string, schema []DbfSchema, codepageTo string) (table DbfTable, e error) {
	if table, e = NewFromSchema(schema, codepageTo); e != nil {
		return
	}
	buf := &bytes.Buffer{}
	if _, e = buf.ReadFrom(src); e != nil {
		return
	}
	var source *DbfTable
	if source, e = NewFromByteArray(buf.Bytes(), codepageFrom); e != nil {
		return
	}

	aliases := make(map[string][]string)
	defs := make(map[string]string)
	exprs := make(map[string]string)
	for _, v := range source.fields {
		aliases[v.name] = append(aliases[v.name], "*"+v.name)
	}
	for _, v := range schema {
		if len(v.Alias) != 0 {
			if _, found := aliases[v.Alias]; found {
				if aliases[v.Alias][0] == "*"+v.Alias {
					aliases[v.Alias][0] = v.FieldName
				} else {
					aliases[v.Alias] = append(aliases[v.Alias], v.FieldName)
				}
			}
		} else if len(v.Default) != 0 {
			defs[v.FieldName] = v.Default
		} else if len(v.Expr) != 0 {
			exprs[v.FieldName] = v.Expr
		}
	}
	for k, v := range aliases {
		if v[0][:1] == "*" {
			aliases[k][0] = ""
		}
	}
	var found bool
out:
	for _, v := range aliases {
		for _, v2 := range v {
			if found = table.HasField(v2); found {
				break out
			}
		}
	}
	if !found {
		return DbfTable{}, &errorEmpty{}
	}
	for i := 0; i < int(source.numberOfRecords); i++ {
		select {
		case <-ctx.Done():
			return DbfTable{}, errors.New("context has been canceled")
		default:
		}

		recno := table.AddNewRecord()
		for _, v := range source.fields {
			for _, v2 := range aliases[v.name] {
				if j, _ := table.FieldIdx(v2); j != -1 {
					var value string
					value, e = source.FieldValueByName(i, v.name)
					if e != nil {
						return
					}
					table.SetFieldValue(recno, j, formatValue(table.fields[j], value))
				}
			}
		}
		if len(defs) > 0 {
			for k, v := range defs {
				table.SetFieldValueByName(recno, k, v)
			}
		}
		if len(exprs) > 0 {
			env := make(map[string]interface{})
			for _, v := range source.fields {
				switch v.fieldType {
				case Numeric:
					if v.decimalPlaces == 0 {
						env[v.name], _ = source.Int64FieldValueByName(i, v.name)
					} else {
						env[v.name], _ = source.Float64FieldValueByName(i, v.name)
					}
				case Float:
					env[v.name], _ = source.Float64FieldValueByName(i, v.name)
				default:
					env[v.name], _ = source.FieldValueByName(i, v.name)
				}
			}
			env["__sprintf"] = fmt.Sprintf
			for k, v := range exprs {
				if j, e := table.FieldIdx(k); e == nil {
					if p, e := expr.Compile(v, expr.Env(env)); e == nil {
						if value, e := expr.Run(p, env); e == nil {
							table.SetFieldValue(recno, j, fmt.Sprintf("%v", value))
						}
					}
				}
			}
		}
	}
	return table, nil
}

// NewFromCSV create schema-based dbf and fill it from csv file
func NewFromCSV(filename string, codepageFrom string, headers bool, skip int, comma rune, schema []DbfSchema, codepageTo string) (DbfTable, error) {
	f, e := os.Open(filename)
	if e != nil {
		return DbfTable{}, e
	}
	defer f.Close()
	return NewFromCSVReader(f, codepageFrom, headers, skip, comma, schema, codepageTo)
}

func NewFromCSVReader(src io.Reader, codepageFrom string, headers bool, skip int, comma rune, schema []DbfSchema, codepageTo string) (table DbfTable, e error) {
	table, e = NewFromSchema(schema, codepageTo)
	if e != nil {
		return
	}
	aliases := make(map[string][]string)
	exprs := make(map[string]string)
	r := csv.NewReader(mahonia.NewDecoder(codepageFrom).NewReader(src))
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
		record, e := r.Read()

		if e == io.EOF {
			break
		}
		if e != nil && len(record) < len(header) {
			return DbfTable{}, e
		}
		if skip--; skip >= 0 {
			continue
		}

		if header == nil {
			if headers {
				header = record
				continue
			}
			for i := 0; i < len(record); i++ {
				header = append(header, "F"+strconv.Itoa(i+1))
			}
		}
		if !fillAliases {
			for _, v := range header {
				aliases[v] = append(aliases[v], v)
			}
			for _, v := range schema {
				if len(v.Alias) != 0 {
					if _, found := aliases[v.Alias]; found {
						if aliases[v.Alias][0] == v.Alias {
							aliases[v.Alias][0] = v.FieldName
						} else {
							aliases[v.Alias] = append(aliases[v.Alias], v.FieldName)
						}
					}
				} else if len(v.Expr) != 0 {
					exprs[v.FieldName] = v.Expr
				}
			}
			var found bool
		out:
			for _, v := range aliases {
				for _, v2 := range v {
					if found = table.HasField(v2); found {
						break out
					}
				}
			}
			if !found {
				return DbfTable{}, &errorEmpty{}
			}
			fillAliases = true
		}

		recno := table.AddNewRecord()
		for i := range record {
			if i < len(header) {
				for _, v := range aliases[header[i]] {
					j, err := table.FieldIdx(v)
					if err != nil {
						continue
					}
					table.SetFieldValue(recno, j, formatValue(table.fields[j], record[i]))
				}
			}
		}
		if len(exprs) > 0 {
			env := make(map[string]interface{})
			for i := 0; i < len(header); i++ {
				env[header[i]] = record[i]
			}
			for k, v := range exprs {
				if j, err := table.FieldIdx(k); err == nil {
					if p, err := expr.Compile(v, expr.Env(env)); err == nil {
						if value, err := expr.Run(p, env); err == nil {
							table.SetFieldValue(recno, j, fmt.Sprintf("%v", value))
						}
					}
				}
			}
		}
	}
	return table, nil
}

/*
NewFromXLS create schema-based dbf from excel file
ошибка форматирования ячеек, закомментировал проблемную часть
xls/col.go:

	func (c *NumberCol) String(wb *WorkBook) []string {
		//if fNo := wb.Xfs[c.Index].formatNo(); fNo != 0 {
		//	t := timeFromExcelTime(c.Float, wb.dateMode == 1)
		//	log.Println(t)
		//	return []string{yymmdd.Format(t, wb.Formats[fNo].str)}
		//}
		return []string{strconv.FormatFloat(c.Float, 'f', -1, 64)}
	}
*/
func NewFromXLS(filename string, codepageFrom string, sheet string, keycolumn, skip int, schema []DbfSchema, codepageTo string) (table DbfTable, e error) {
	table, e = NewFromSchema(schema, codepageTo)
	if e != nil {
		return
	}
	xl, e := xls.Open(filename, codepageFrom)
	if e != nil {
		return DbfTable{}, e
	}

	for i := 0; i < xl.NumSheets(); i++ {
		sh := xl.GetSheet(i)
		if strings.EqualFold(sh.Name, sheet) {
			aliases := make(map[string][]string)
			var header []string
			for j := 1 + skip; j <= int(sh.MaxRow); j++ {
				r := sh.Row(j)
				if _, e := strconv.ParseFloat(r.Col(keycolumn), 64); e != nil {
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
						aliases[v] = append(aliases[v], v)
					}
					for _, v := range schema {
						if len(v.Alias) != 0 {
							if _, found := aliases[v.Alias]; found {
								if aliases[v.Alias][0] == v.Alias {
									aliases[v.Alias][0] = v.FieldName
								} else {
									aliases[v.Alias] = append(aliases[v.Alias], v.FieldName)
								}
							}
						}
					}
					var found bool
				out:
					for _, v := range aliases {
						for _, v2 := range v {
							if found = table.HasField(v2); found {
								break out
							}
						}
					}
					if !found {
						return DbfTable{}, &errorEmpty{}
					}
				}

				recno := table.AddNewRecord()
				for k := range record {
					if k < len(header) {
						for _, v := range aliases[header[k]] {
							l, e := table.FieldIdx(v)
							if e != nil {
								continue
							}
							table.SetFieldValue(recno, l, formatValue(table.fields[l], record[k]))
						}
					}
				}
			}
			return table, nil
		}
	}
	return DbfTable{}, fmt.Errorf("no sheet named %s", sheet)
}

func NewFromJSON(ctx context.Context, filename string, codepageFrom string, schema []DbfSchema, codepageTo string) ([]DbfTable, error) {
	f, e := os.Open(filename)
	if e != nil {
		return nil, e
	}
	defer f.Close()
	return NewFromJSONReader(ctx, f, codepageFrom, schema, codepageTo)
}

func NewFromJSONReader(ctx context.Context, src io.Reader, codepageFrom string, schema []DbfSchema, codepageTo string) (tables []DbfTable, e error) {
	r := json.NewDecoder(src)
	//r.CharsetReader = charset.NewReaderLabel
	aliases := make(map[string][]DbfSchema)
	exprs := make(map[string]string)
	for _, v := range schema {
		if len(v.Alias) != 0 {
			aliases[strings.ToLower(v.Alias)] = append(aliases[strings.ToLower(v.Alias)], v)
		} else if len(v.Expr) != 0 {
			exprs[v.FieldName] = v.Expr
		}
	}
	const (
		lvlTables = iota
		lvlTable
		lvlRecord
	)
	var (
		//curtag   string
		errFound bool
		//errText  string
		//recno    int = -1
		env = make(map[string]interface{})
	)

	/*min := func(v1, v2 int64) int64 {
		if v1 <= v2 {
			return v1
		}
		return v2
	}*/
	/*utf2x := func(s, codepage string) string {
		return mahonia.NewEncoder(codepage).ConvertString(s)
	}*/
	doExpr := func(table *DbfTable, recno int) {
		if recno != -1 && len(exprs) > 0 {
			for k, v := range exprs {
				if j, err := table.FieldIdx(k); err == nil {
					p, err := expr.Compile(v, expr.Env(env))
					if err != nil {
						return
					}
					if value, err := expr.Run(p, env); err == nil {
						table.SetFieldValue(recno, j, fmt.Sprintf("%v", value))
					}
				}
			}
		}
	}
	/*addNew := func(table *DbfTable) (i int, e error) {
		if errFound {
			if len(errText) > 0 {
				return -1, errors.New(utf2x(errText, codepageFrom))
			}
			return -1, errors.New("unknown error")
		}

		doExpr(table, recno)
		i = table.AddNewRecord()
		env = make(map[string]interface{})
		for _, v := range aliases {
			for _, v2 := range v {
				if v2.Header {
					if e := table.SetFieldValueByName(i, v2.FieldName, v2.Default); e != nil {
						return -1, e
					}
				}
			}
		}
		return
	}*/

	var (
		start = true
		token json.Token
		level = lvlTables
		table DbfTable
		//recno int
	)
	for {
		select {
		case <-ctx.Done():
			return nil, errors.New("context has been canceled")
		default:
		}

		token, e = r.Token()
		if e == io.EOF {
			break
		}
		if e != nil {
			return
		}
		switch token.(type) {
		case json.Delim:
			switch token {
			case "[":
				level += 1
			case "]":
				level -= 1
			case "{":
				if start {
					level = lvlTable
				}
				switch level {
				case lvlTable:
					table, e = NewFromSchema(schema, codepageTo)
					if e != nil {
						return
					}
				case lvlRecord:
					//recno = table.AddNewRecord()
				}
			case "}":
			}
			start = false
		}

	}

	/*switch t := token.(type) {
	case xml.StartElement:
		curtag = strings.ToLower(t.Name.Local)
		if v, found := aliases[curtag]; found && v[0].FieldName == "__NEWTABLE" {
			if table.NumberOfRecords() > 0 {
				doExpr(&table, table.NumberOfRecords()-1)
				tables = append(tables, table)
			}
			table, e = NewFromSchema(schema, codepageTo)
			if e != nil {
				return
			}
			recno = -1
		}

		var b2 bool
		v, b := aliases[curtag+":"]
		if !b {
			v, b2 = aliases[curtag]
		}
		if (b || b2) && (v[0].FieldName == "__NEW") {
			if len(table.Fields()) == 0 {
				if table, e = NewFromSchema(schema, codepageTo); e != nil {
					return
				}
			}
			if recno, e = addNew(&table); e != nil {
				return nil, e
			}
			if b {
				for _, a := range t.Attr {
					for _, v2 := range aliases[curtag+":"+a.Name.Local] {
						if v2.FieldName != "__NEW" && v2.FieldName != "" && a.Value != "\n\t" {
							l, _ := table.FieldIdx(v2.FieldName)
							table.SetFieldValue(recno, l, formatValue(table.fields[l], a.Value))
						}
					}
					env[curtag+"_"+a.Name.Local] = strings.Trim(a.Value, " ")
				}
			}
		}
	case xml.CharData:
		if curtag != "" {
			for i, v := range aliases[curtag] {
				switch {
				case recno == -1 && v.Header:
					if len(table.Fields()) == 0 {
						var tmp DbfTable
						if tmp, e = NewFromSchema(schema, codepageTo); e != nil {
							return
						}
						v.Default = formatValue(tmp.FieldByName(v.FieldName), string(t))
					} else {
						v.Default = formatValue(table.FieldByName(v.FieldName), string(t))
					}
					aliases[curtag][i] = v
				case v.FieldName == "__ERR" && v.Default == string(t):
					errFound = true
				case v.FieldName == "__ERRTEXT":
					return nil, errors.New(string(t))
				case v.FieldName != "" && v.FieldName[:min(5, int64(len(v.FieldName)))] != "__NEW" && string(t) != "\n\t":
					if len(table.Fields()) != 0 && recno > -1 {
						l, _ := table.FieldIdx(v.FieldName)
						table.SetFieldValue(recno, l, formatValue(table.fields[l], string(t)))
					}
				}
			}
			env[curtag] = strings.Trim(string(t), " ")
			curtag = ""
		}
	}*/
	if table.NumberOfRecords() > 0 {
		doExpr(&table, table.NumberOfRecords()-1)
		tables = append(tables, table)
	}
	if len(tables) == 0 || errFound {
		table, _ = NewFromSchema(schema, codepageTo)
		tables = []DbfTable{table}
		return tables, errors.New("unknown error")
	}
	return tables, nil
}

func NewFromXML(ctx context.Context, filename string, codepageFrom string, schema []DbfSchema, codepageTo string) ([]DbfTable, error) {
	f, e := os.Open(filename)
	if e != nil {
		return nil, e
	}
	defer f.Close()
	return NewFromXMLReader(ctx, f, codepageFrom, schema, codepageTo)
}

// NewFromXMLReader create dbf from XML
func NewFromXMLReader(ctx context.Context, src io.Reader, codepageFrom string, schema []DbfSchema, codepageTo string) (tables []DbfTable, e error) {
	r := xml.NewDecoder(src)
	r.CharsetReader = charset.NewReaderLabel
	aliases := make(map[string][]DbfSchema)
	exprs := make(map[string]string)
	for _, v := range schema {
		if len(v.Alias) != 0 {
			aliases[strings.ToLower(v.Alias)] = append(aliases[strings.ToLower(v.Alias)], v)
		} else if len(v.Expr) != 0 {
			exprs[v.FieldName] = v.Expr
		}
	}
	var (
		curtag   string
		errFound bool
		errText  string
		recno    int = -1
		env          = make(map[string]interface{})
	)

	min := func(v1, v2 int64) int64 {
		if v1 <= v2 {
			return v1
		}
		return v2
	}
	utf2x := func(s, codepage string) string {
		return mahonia.NewEncoder(codepage).ConvertString(s)
	}
	doExpr := func(table *DbfTable, recno int) {
		if recno != -1 && len(exprs) > 0 {
			for k, v := range exprs {
				if j, err := table.FieldIdx(k); err == nil {
					p, err := expr.Compile(v, expr.Env(env))
					if err != nil {
						return
					}
					if value, err := expr.Run(p, env); err == nil {
						table.SetFieldValue(recno, j, fmt.Sprintf("%v", value))
					}
				}
			}
		}
	}
	addNew := func(table *DbfTable) (i int, e error) {
		if errFound {
			if len(errText) > 0 {
				return -1, errors.New(utf2x(errText, codepageFrom))
			}
			return -1, errors.New("unknown error")
		}

		doExpr(table, recno)
		i = table.AddNewRecord()
		env = make(map[string]interface{})
		for _, v := range aliases {
			for _, v2 := range v {
				if v2.Header {
					if e := table.SetFieldValueByName(i, v2.FieldName, v2.Default); e != nil {
						return -1, e
					}
				}
			}
		}
		return
	}

	var table DbfTable
	for {
		select {
		case <-ctx.Done():
			return nil, errors.New("context has been canceled")
		default:
		}

		token, _ := r.Token()
		if token == nil {
			break
		}

		switch t := token.(type) {
		case xml.StartElement:
			curtag = strings.ToLower(t.Name.Local)
			if v, found := aliases[curtag]; found && v[0].FieldName == "__NEWTABLE" {
				if table.NumberOfRecords() > 0 {
					doExpr(&table, table.NumberOfRecords()-1)
					tables = append(tables, table)
				}
				table, e = NewFromSchema(schema, codepageTo)
				if e != nil {
					return
				}
				recno = -1
			}

			var b2 bool
			v, b := aliases[curtag+":"]
			if !b {
				v, b2 = aliases[curtag]
			}
			if (b || b2) && (v[0].FieldName == "__NEW") {
				if len(table.Fields()) == 0 {
					if table, e = NewFromSchema(schema, codepageTo); e != nil {
						return
					}
				}
				if recno, e = addNew(&table); e != nil {
					return nil, e
				}
				if b {
					for _, a := range t.Attr {
						for _, v2 := range aliases[curtag+":"+a.Name.Local] {
							if v2.FieldName != "__NEW" && v2.FieldName != "" && a.Value != "\n\t" {
								l, _ := table.FieldIdx(v2.FieldName)
								table.SetFieldValue(recno, l, formatValue(table.fields[l], a.Value))
							}
						}
						env[curtag+"_"+a.Name.Local] = strings.Trim(a.Value, " ")
					}
				}
			}
		case xml.CharData:
			if curtag != "" {
				for i, v := range aliases[curtag] {
					switch {
					case recno == -1 && v.Header:
						if len(table.Fields()) == 0 {
							var tmp DbfTable
							if tmp, e = NewFromSchema(schema, codepageTo); e != nil {
								return
							}
							v.Default = formatValue(tmp.FieldByName(v.FieldName), string(t))
						} else {
							v.Default = formatValue(table.FieldByName(v.FieldName), string(t))
						}
						aliases[curtag][i] = v
					case v.FieldName == "__ERR" && v.Default == string(t):
						errFound = true
					case v.FieldName == "__ERRTEXT":
						return nil, errors.New(string(t))
					case v.FieldName != "" && v.FieldName[:min(5, int64(len(v.FieldName)))] != "__NEW" && string(t) != "\n\t":
						if len(table.Fields()) != 0 && recno > -1 {
							l, _ := table.FieldIdx(v.FieldName)
							table.SetFieldValue(recno, l, formatValue(table.fields[l], string(t)))
						}
					}
				}
				env[curtag] = strings.Trim(string(t), " ")
				curtag = ""
			}
		}
	}
	if table.NumberOfRecords() > 0 {
		doExpr(&table, table.NumberOfRecords()-1)
		tables = append(tables, table)
	}
	if len(tables) == 0 || errFound {
		table, _ = NewFromSchema(schema, codepageTo)
		tables = []DbfTable{table}
		return tables, errors.New("unknown error")
	}
	return tables, nil
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

		if len(s) <= offset+11 {
			return nil, errors.New("createDbfTable: index out of range")
		}

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

// SaveFile save file on disk
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

// SaveCSV translate dbf to csv format
func (dt *DbfTable) SaveCSV(filename string, codepage string, comma rune, headers bool) (e error) {
	f, e := os.Create(filename)
	if e != nil {
		return
	}
	defer func() {
		f.Close()
		if e != nil {
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
		if e = w.Write(fieldRow); e != nil {
			return
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

		if e = w.Write(row); e != nil {
			return
		}
		w.Flush()
	}
	return nil
}
