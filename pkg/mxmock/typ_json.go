package mxmock

import (
	"encoding/json"
	"strings"

	"github.com/brianvoe/gofakeit/v6"
	"github.com/ymatrix-data/mxbench/internal/util/log"
)

type JSON struct {
	BaseType

	Name string `json:"name"`
	Age  int    `json:"number" fake:"{number:1,100}"`
}

func NewJSON(colName string) Type {
	return &JSON{
		BaseType: NewBaseType(colName),
	}
}

func (j *JSON) Random(keys ...string) string {
	// print key and colName

	log.Info("json colName: %s, keys: %+v", j.colName, keys)
	for _, key := range keys {
		if key != j.colName {
			continue
		}
		_ = gofakeit.Struct(j)
		b, _ := json.Marshal(j)
		return string(b)
	}
	return "{}"
}

func (j *JSON) ValueRange() map[string]*ValueRange {
	return nil
}

type JSONs struct {
	BaseType
	jsons []*JSON
}

func NewJSONs(colName string) Type {
	return &JSONs{
		BaseType: NewBaseType(colName),
		jsons:    []*JSON{NewJSON(colName).(*JSON)},
	}
}

func (js JSONs) Random(keys ...string) string {
	log.Info("json colName: %s, keys: %+v", js.colName, keys)

	for _, key := range keys {
		if key != js.colName {
			continue
		}
		a := []string{}
		for _, j := range js.jsons {
			a = append(a, j.Random(keys...))
		}
		return "{\"" + strings.ReplaceAll(strings.Join(a, ","), "\"", "\\\"") + "\"}"
	}
	return "{}"
}

func (js JSONs) Parse(string) {}

func (js *JSONs) ValueRange() map[string]*ValueRange {
	// TODO: implement
	return nil
}

type JSONB = JSON
type JSONBs = JSONs
