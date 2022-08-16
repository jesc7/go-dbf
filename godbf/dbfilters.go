package godbf

import (
	"bufio"
	"os"
	"regexp"
	"strings"
)

type DbfFilters struct {
	Repls []struct {
		Name string `json:"name"`
		F    string `json:"f"`
		R    string `json:"r"`
	} `json:"repls"`
	File string `json:"file"`
}

type rpl struct {
	r  string
	re *regexp.Regexp
}

type Fltrs struct {
	rm  map[string][]rpl
	buf map[string]string
}

func UseFilter(table *DbfTable, f DbfFilters) {
	if fs := NewFltrs(f); !fs.Empty() {
		for i := 0; i < table.NumberOfRecords(); i++ {
			fs.Clear()
			for _, field := range table.Fields() {
				s, _ := table.FieldValueByName(i, field.Name())
				if s1 := fs.Replace(field.Name(), s); s != s1 {
					table.SetFieldValueByName(i, field.Name(), s1)
				}
			}
		}
	}
}

func NewFltrs(cfg DbfFilters) *Fltrs {
	fs := &Fltrs{}
	fs.buf = make(map[string]string)
	fs.rm = make(map[string][]rpl)
	fs.prepare(cfg)
	return fs
}

func (f *Fltrs) prepare(cfg DbfFilters) {
	tmp := make(map[string]struct{})
	addFltr := func(key, s, r string) {
		if _, ok := tmp[key+s]; ok {
			return
		}
		if re, e := regexp.Compile(s); e == nil {
			f.rm[key] = append(f.rm[key], rpl{r, re})
			tmp[key+s] = struct{}{}
		}
	}
	for _, v := range cfg.Repls {
		addFltr(v.Name, v.F, v.R)
	}
	if file, e := os.Open(cfg.File); e == nil {
		defer file.Close()
		sc := bufio.NewScanner(file)
		for sc.Scan() {
			if sl := strings.Split(sc.Text(), "\t"); len(sl) == 3 && len(sl[0]) > 0 {
				addFltr(sl[0], sl[1], sl[2])
			}
		}
	}
}

func (f *Fltrs) Empty() bool {
	return len(f.rm) == 0
}

func (f *Fltrs) Clear() {
	f.buf = make(map[string]string)
}

func (f *Fltrs) Replace(key, value string) string {
	if value, ok := f.buf[key]; ok {
		return value
	}
	for _, v := range f.rm[key] {
		value = v.re.ReplaceAllString(value, v.r)
	}
	f.buf[key] = value
	return value
}
