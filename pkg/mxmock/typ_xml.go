package mxmock

import (
	"encoding/xml"
	"strings"

	"github.com/brianvoe/gofakeit/v6"
)

type XML struct {
	BaseType

	XMLName xml.Name `xml:"mxmock"`
	Name    string   `xml:"name,attr"`
	Age     int      `xml:"number" fake:"{number:1,100}"`
}

func NewXML(colName string) Type {
	return &XML{
		BaseType: NewBaseType(colName),
	}
}

func (x *XML) Random(keys ...string) string {
	for _, key := range keys {
		if key != x.colName {
			continue
		}
		_ = gofakeit.Struct(x)
		b, _ := xml.MarshalIndent(x, "  ", "    ")
		return xml.Header + string(b)
	}
	return ""
}

func (x *XML) ValueRange() map[string]*ValueRange {
	return nil
}

type XMLs struct {
	BaseType
	xmls []*XML
}

func NewXMLs(colName string) Type {
	return &XMLs{
		BaseType: NewBaseType(colName),
		xmls:     []*XML{NewXML(colName).(*XML)},
	}
}

func (xs XMLs) Random(keys ...string) string {
	for _, key := range keys {
		if key != xs.colName {
			continue
		}
		a := []string{}
		for _, x := range xs.xmls {
			a = append(a, x.Random(keys...))
		}
		return "{\"" + strings.ReplaceAll(strings.Join(a, ","), "\"", "\\\"") + "\"}"
	}
	return "{}"
}

func (xs XMLs) Parse(string) {}

func (xs *XMLs) ValueRange() map[string]*ValueRange {
	return nil
}
