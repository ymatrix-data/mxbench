package mxmock

import (
	"fmt"
	"strings"

	"github.com/brianvoe/gofakeit/v6"
)

type PgLsn struct {
	BaseType
}

func NewPgLsn(colName string) Type {
	return &PgLsn{
		BaseType: NewBaseType(colName),
	}
}

func (p *PgLsn) Random(keys ...string) string {
	for _, key := range keys {
		if key != p.colName {
			continue
		}
		return fmt.Sprintf("%02x/%02x",
			gofakeit.Word(), gofakeit.Word())
	}
	return ""
}

func (p *PgLsn) ValueRange() map[string]*ValueRange {
	return nil
}

type PgLsns struct {
	BaseType
	pgLsns []*PgLsn
}

func NewPgLsns(colName string) Type {
	return &PgLsns{
		BaseType: NewBaseType(colName),
		pgLsns:   []*PgLsn{NewPgLsn(colName).(*PgLsn)},
	}
}

func (ps PgLsns) Random(keys ...string) string {
	for _, key := range keys {
		if key != ps.colName {
			continue
		}
		a := []string{}
		for _, p := range ps.pgLsns {
			a = append(a, p.Random(keys...))
		}
		return "{" + strings.Join(a, ",") + "}"
	}
	return "{}"
}

func (ps PgLsns) Parse(string) {}

func (ps PgLsns) ValueRange() map[string]*ValueRange {
	return nil
}
