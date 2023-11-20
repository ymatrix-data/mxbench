package mxmock

import (
	"strings"

	"github.com/brianvoe/gofakeit/v6"
)

type TSQuery struct {
	BaseType
}

func NewTSQuery(colName string) Type {
	return &TSQuery{
		BaseType: NewBaseType(colName),
	}
}

func (t *TSQuery) Random(keys ...string) string {
	for _, key := range keys {
		if key != t.colName {
			continue
		}
		number := gofakeit.Number(1, 9999)
		number = number % 5
		if number == 0 {
			return gofakeit.Word() + " & " + gofakeit.Word()
		} else if number == 1 {
			return gofakeit.Word() + " | " + gofakeit.Word()
		} else if number == 2 {
			return " ! " + gofakeit.Word() + " & " + gofakeit.Word()
		} else if number == 3 {
			return gofakeit.Word() + " & " + gofakeit.Word() + "  & ! " + gofakeit.Word()
		} else {
			return gofakeit.Word() + " & ( " + gofakeit.Word() + " | " + gofakeit.Word() + " )"
		}
	}
	return ""
}

func (t *TSQuery) ValueRange() map[string]*ValueRange {
	return nil
}

type TSQueries struct {
	BaseType
	tsQueries []*TSQuery
}

func NewTSQueries(colName string) Type {
	return &TSQueries{
		BaseType:  NewBaseType(colName),
		tsQueries: []*TSQuery{NewTSQuery(colName).(*TSQuery)},
	}
}

func (ts TSQueries) Random(keys ...string) string {
	for _, key := range keys {
		if key != ts.colName {
			continue
		}
		a := []string{}
		for _, t := range ts.tsQueries {
			a = append(a, t.Random(keys...))
		}
		return "{" + strings.Join(a, ",") + "}"
	}
	return "{}"
}

func (ts TSQueries) Parse(string) {}

func (ts *TSQueries) ValueRange() map[string]*ValueRange {
	return nil
}

type TSVector struct {
	BaseType
}

func NewTsVector(colName string) Type {
	return &TSVector{
		BaseType: NewBaseType(colName),
	}
}

func (t *TSVector) Random(keys ...string) string {
	for _, key := range keys {
		if key != t.colName {
			continue
		}
		return gofakeit.Sentence(100)
	}
	return ""
}

func (t *TSVector) ValueRange() map[string]*ValueRange {
	return nil
}

type TSVectors struct {
	BaseType
	tsVectors []*TSVector
}

func NewTsVectors(colName string) Type {
	return &TSVectors{
		BaseType:  NewBaseType(colName),
		tsVectors: []*TSVector{NewTsVector(colName).(*TSVector)},
	}
}

func (ts TSVectors) Random(keys ...string) string {
	for _, key := range keys {
		if key != ts.colName {
			continue
		}
		a := []string{}
		for _, t := range ts.tsVectors {
			a = append(a, t.Random(keys...))
		}
		return "{" + strings.Join(a, ",") + "}"
	}
	return "{}"
}

func (ts TSVectors) Parse(string) {}

func (ts *TSVectors) ValueRange() map[string]*ValueRange {
	return nil
}
