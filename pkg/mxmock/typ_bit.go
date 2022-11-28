package mxmock

import (
	"strings"

	"github.com/brianvoe/gofakeit/v6"
)

type Bit struct {
	BaseType
	Length int
}

func NewBit(colName string) Type {
	return &Bit{
		BaseType: NewBaseType(colName),
	}
}

func (b *Bit) Random(keys ...string) string {
	for _, key := range keys {
		if key != b.colName {
			continue
		}
		var bitValue string
		for i := 0; i < b.Length; i++ {
			if gofakeit.Bool() {
				bitValue = bitValue + "1"
			} else {
				bitValue = bitValue + "0"
			}
		}
		return bitValue
	}
	return ""
}

func (b *Bit) Parse(td string) {
	b.Length, _ = b.CharLen(td)
}

type Bits struct {
	BaseType
	bits []*Bit
}

func NewBits(colName string) Type {
	return &Bits{
		BaseType: NewBaseType(colName),
		bits:     []*Bit{NewBit(colName).(*Bit)},
	}
}

func (bs Bits) Random(keys ...string) string {
	for _, key := range keys {
		if key != bs.colName {
			continue
		}
		a := []string{}
		for _, b := range bs.bits {
			a = append(a, b.Random(keys...))
		}
		return "{" + strings.Join(a, ",") + "}"
	}
	return "{}"
}

func (bs Bits) Parse(td string) {
	for _, b := range bs.bits {
		b.Parse(td)
	}
}

type VarBit = Bit

type VarBits = Bits
