package mxmock

import (
	"fmt"
	"strings"

	"github.com/brianvoe/gofakeit/v6"
)

type Bool struct {
	BaseType
}

func NewBool(colName string) Type {
	return &Bool{
		BaseType: NewBaseType(colName),
	}
}

func (b *Bool) Random(keys ...string) string {
	for _, key := range keys {
		if key != b.colName {
			continue
		}
		return fmt.Sprintf("%t", gofakeit.Bool())
	}
	return ""
}

type Bools struct {
	BaseType
	bools []*Bool
}

func NewBools(colName string) Type {
	return &Bools{
		BaseType: NewBaseType(colName),
		bools:    []*Bool{NewBool(colName).(*Bool)},
	}
}

func (bs Bools) Random(keys ...string) string {
	for _, key := range keys {
		if key != bs.colName {
			continue
		}
		a := []string{}
		for _, b := range bs.bools {
			a = append(a, b.Random(keys...))
		}
		return "{" + strings.Join(a, ",") + "}"
	}
	return "{}"
}

func (bs Bools) Parse(string) {}
