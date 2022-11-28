package mxmock

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/brianvoe/gofakeit/v6"
)

type TxidSnapshot struct {
	BaseType
}

func NewTxidSnapshot(colName string) Type {
	return &TxidSnapshot{
		BaseType: NewBaseType(colName),
	}
}

func (t *TxidSnapshot) Random(keys ...string) string {
	for _, key := range keys {
		if key != t.colName {
			continue
		}
		x, _ := strconv.Atoi(gofakeit.DigitN(8))
		y, _ := strconv.Atoi(gofakeit.DigitN(8))
		if x > y {
			return fmt.Sprintf("%v:%v:", y, x)
		}
		return fmt.Sprintf("%v:%v:", x, y)
	}
	return ""
}

type TxidSnapshots struct {
	BaseType
	txidSnapshots []*TxidSnapshot
}

func NewTxidSnapshots(colName string) Type {
	return &TxidSnapshots{
		BaseType:      NewBaseType(colName),
		txidSnapshots: []*TxidSnapshot{NewTxidSnapshot(colName).(*TxidSnapshot)},
	}
}

func (ts TxidSnapshots) Random(keys ...string) string {
	for _, key := range keys {
		if key != ts.colName {
			continue
		}
		a := []string{}
		for _, t := range ts.txidSnapshots {
			a = append(a, t.Random(keys...))
		}
		return "{" + strings.Join(a, ",") + "}"
	}
	return "{}"
}

func (ts TxidSnapshots) Parse(string) {}
