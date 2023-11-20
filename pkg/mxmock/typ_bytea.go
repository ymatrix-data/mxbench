package mxmock

import (
	"fmt"
	"strings"

	"github.com/brianvoe/gofakeit/v6"
)

type ByteA struct {
	BaseType
}

func NewByteA(colName string) Type {
	return &ByteA{
		BaseType: NewBaseType(colName),
	}
}

func (b ByteA) Random(keys ...string) string {
	for _, key := range keys {
		if key != b.colName {
			continue
		}
		result := make([]byte, gofakeit.Number(0, 1024)+1)
		for i := range result {
			result[i] = byte(gofakeit.Number(0, 255))
		}
		return fmt.Sprintf("%v", result)
	}
	return ""
}

func (b *ByteA) ValueRange() map[string]*ValueRange {
	return nil
}

type ByteAs struct {
	BaseType
	byteAs []*ByteA
}

func NewByteAs(colName string) Type {
	return &ByteAs{
		BaseType: NewBaseType(colName),
		byteAs:   []*ByteA{NewByteA(colName).(*ByteA)},
	}
}
func (bs ByteAs) Random(keys ...string) string {
	for _, key := range keys {
		if key != bs.colName {
			continue
		}
		a := []string{}
		for _, b := range bs.byteAs {
			a = append(a, b.Random(keys...))
		}
		return "{" + strings.Join(a, ",") + "}"
	}
	return "{}"
}

func (bs ByteAs) Parse(string) {}

func (bs *ByteAs) ValueRange() map[string]*ValueRange {
	return nil
}
