package mxmock

import (
	"strings"

	"github.com/brianvoe/gofakeit/v6"
)

type Text struct {
	BaseType
}

func NewText(colName string) Type {
	return &Text{
		BaseType: NewBaseType(colName),
	}
}

func (t *Text) Random(keys ...string) string {
	for _, key := range keys {
		if key != t.colName {
			continue
		}
		return gofakeit.Sentence(30)
	}
	return ""
}

func (t *Text) ValueRange() map[string]*ValueRange {
	return nil
}

type Texts struct {
	BaseType
	texts []*Text
}

func NewTexts(colName string) Type {
	return &Texts{
		BaseType: NewBaseType(colName),
		texts:    []*Text{NewText(colName).(*Text)},
	}
}

func (ts Texts) Random(keys ...string) string {
	for _, key := range keys {
		if key != ts.colName {
			continue
		}
		a := []string{}
		for _, t := range ts.texts {
			a = append(a, t.Random(keys...))
		}
		return "{" + strings.Join(a, ",") + "}"
	}
	return "{}"
}

func (ts Texts) Parse(string) {}

func (ts *Texts) ValueRange() map[string]*ValueRange {
	return nil
}

// TODO
type CiText struct{}
