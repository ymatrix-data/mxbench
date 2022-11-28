package mxmock

import (
	"strings"

	"github.com/brianvoe/gofakeit/v6"
)

type MacAddr struct {
	BaseType
}

func NewMacAddr(colName string) Type {
	return &MacAddr{
		BaseType: NewBaseType(colName),
	}
}

func (m *MacAddr) Random(keys ...string) string {
	for _, key := range keys {
		if key != m.colName {
			continue
		}
		return gofakeit.MacAddress()
	}
	return ""
}

type MacAddrs struct {
	BaseType
	macAddrs []*MacAddr
}

func NewMacAddrs(colName string) Type {
	return &MacAddrs{
		BaseType: NewBaseType(colName),
		macAddrs: []*MacAddr{NewMacAddr(colName).(*MacAddr)},
	}
}

func (ms MacAddrs) Random(keys ...string) string {
	for _, key := range keys {
		if key != ms.colName {
			continue
		}
		a := []string{}
		for _, m := range ms.macAddrs {
			a = append(a, m.Random(keys...))
		}
		return "{" + strings.Join(a, ",") + "}"
	}
	return "{}"
}

func (ms MacAddrs) Parse(string) {}
