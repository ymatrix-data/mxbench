package mxmock

import (
	"fmt"
	"strings"

	"github.com/brianvoe/gofakeit/v6"
)

type Int2 struct {
	BaseType
}

func NewInt2(colName string) Type {
	return &Int2{
		BaseType: NewBaseType(colName),
	}
}

func (i2 *Int2) Random(keys ...string) string {
	for _, key := range keys {
		if key != i2.colName {
			continue
		}
		return fmt.Sprintf("%d", gofakeit.Number(-2767, 2767))
	}
	return ""
}

type Int2s struct {
	BaseType
	int2s []*Int2
}

func NewInt2s(colName string) Type {
	return &Int2s{
		BaseType: NewBaseType(colName),
		int2s:    []*Int2{NewInt2(colName).(*Int2)},
	}
}

func (i2s Int2s) Random(keys ...string) string {
	for _, key := range keys {
		if key != i2s.colName {
			continue
		}
		a := []string{}
		for _, i2 := range i2s.int2s {
			a = append(a, i2.Random(keys...))
		}
		return "{" + strings.Join(a, ",") + "}"
	}
	return "{}"
}

func (i2s Int2s) Parse(string) {}

type Int4 struct {
	BaseType
}

func NewInt4(colName string) Type {
	return &Int4{
		BaseType: NewBaseType(colName),
	}
}

func (i4 *Int4) Random(keys ...string) string {
	for _, key := range keys {
		if key != i4.colName {
			continue
		}
		return fmt.Sprintf("%d", gofakeit.Number(-7483647, 7483647))
	}
	return ""
}

type Int4s struct {
	BaseType
	int4s []*Int4
}

func NewInt4s(colName string) Type {
	return &Int4s{
		BaseType: NewBaseType(colName),
		int4s:    []*Int4{NewInt4(colName).(*Int4)},
	}
}

func (i4s Int4s) Random(keys ...string) string {
	for _, key := range keys {
		if key != i4s.colName {
			continue
		}
		a := []string{}
		for _, i4 := range i4s.int4s {
			a = append(a, i4.Random(keys...))
		}
		return "{" + strings.Join(a, ",") + "}"
	}
	return "{}"
}

func (i4s Int4s) Parse(string) {}

type Int8 struct {
	BaseType
}

func NewInt8(colName string) Type {
	return &Int8{
		BaseType: NewBaseType(colName),
	}
}

func (i8 *Int8) Random(keys ...string) string {
	for _, key := range keys {
		if key != i8.colName {
			continue
		}
		return fmt.Sprintf("%d", gofakeit.Number(-372036854775807, 372036854775807))
	}
	return ""
}

type Int8s struct {
	BaseType
	int8s []*Int8
}

func NewInt8s(colName string) Type {
	return &Int8s{
		BaseType: NewBaseType(colName),
		int8s:    []*Int8{NewInt8(colName).(*Int8)},
	}
}

func (i8s Int8s) Random(keys ...string) string {
	for _, key := range keys {
		if key != i8s.colName {
			continue
		}
		a := []string{}
		for _, i8 := range i8s.int8s {
			a = append(a, i8.Random(keys...))
		}
		return "{" + strings.Join(a, ",") + "}"
	}
	return "{}"
}

func (i8s Int8s) Parse(string) {}

type Oid struct {
	BaseType
}

func NewOid(colName string) Type {
	return &Oid{
		BaseType: NewBaseType(colName),
	}
}

func (id *Oid) Random(keys ...string) string {
	for _, key := range keys {
		if key != id.colName {
			continue
		}
		return fmt.Sprintf("%d", gofakeit.Number(-7483647, 7483647))
	}
	return ""
}
