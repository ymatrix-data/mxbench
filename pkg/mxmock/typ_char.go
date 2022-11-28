package mxmock

import (
	"strings"

	"github.com/brianvoe/gofakeit/v6"
)

type BPChar struct {
	BaseType
	Length int
}

func NewBPChar(colName string) Type {
	return &BPChar{
		BaseType: NewBaseType(colName),
	}
}

func (bp *BPChar) Random(keys ...string) string {
	for _, key := range keys {
		if key != bp.colName {
			continue
		}
		return gofakeit.LetterN(uint(bp.Length))
	}
	return ""
}

func (bp *BPChar) Parse(td string) {
	bp.Length, _ = bp.CharLen(td)
}

type BPChars struct {
	BaseType
	bpChars []*BPChar
}

func NewBPChars(colName string) Type {
	return &BPChars{
		BaseType: NewBaseType(colName),
		bpChars:  []*BPChar{NewBPChar(colName).(*BPChar)},
	}
}

func (bps BPChars) Random(keys ...string) string {
	for _, key := range keys {
		if key != bps.colName {
			continue
		}
		a := []string{}
		for _, bp := range bps.bpChars {
			a = append(a, bp.Random(keys...))
		}
		return "{" + strings.Join(a, ",") + "}"
	}
	return "{}"
}

func (bps BPChars) Parse(td string) {
	for _, bp := range bps.bpChars {
		bp.Parse(td)
	}
}

type VarChar struct {
	BaseType
	Length int
}

func NewVarChar(colName string) Type {
	return &VarChar{
		BaseType: NewBaseType(colName),
	}
}

func (v *VarChar) Random(keys ...string) string {
	for _, key := range keys {
		if key != v.colName {
			continue
		}
		return gofakeit.LetterN(uint(v.Length))
	}
	return ""
}

func (v *VarChar) Parse(td string) {
	v.Length, _ = v.CharLen(td)
}

type VarChars struct {
	BaseType
	varChars []*VarChar
}

func NewVarChars(colName string) Type {
	return &VarChars{
		BaseType: NewBaseType(colName),
		varChars: []*VarChar{NewVarChar(colName).(*VarChar)},
	}
}

func (vs VarChars) Random(keys ...string) string {
	for _, key := range keys {
		if key != vs.colName {
			continue
		}
		a := []string{}
		for _, v := range vs.varChars {
			a = append(a, v.Random())
		}
		return "{" + strings.Join(a, ",") + "}"
	}
	return "{}"
}

func (vs VarChars) Parse(td string) {
	for _, v := range vs.varChars {
		v.Parse(td)
	}
}
