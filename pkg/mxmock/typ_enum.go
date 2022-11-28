package mxmock

import (
	"github.com/brianvoe/gofakeit/v6"
	"github.com/jmoiron/sqlx"
)

type Enum struct {
	BaseType
	vals []string
}

type EnumVal struct {
	Name   string `db:"enum_name"`
	Schema string `db:"enum_schema"`
	Value  string `db:"enum_value"`
}

type EnumVals []*EnumVal

func (vs EnumVals) Val() []string {
	r := make([]string, len(vs))
	for idx, v := range vs {
		r[idx] = v.Value
	}
	return r
}

func NewEnum(dt string, conn *sqlx.DB) (*Enum, error) {
	rows, err := conn.Query(_SELECT_ENUM_VALUES, dt)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var vals EnumVals
	for rows.Next() {
		var item EnumVal
		err = rows.Scan(&item.Schema, &item.Name, &item.Value)
		if err != nil {
			return nil, err
		}
		vals = append(vals, &item)
	}

	if err = rows.Err(); err != nil {
		return nil, err
	}

	return &Enum{
		vals: vals.Val(),
	}, nil
}

func (e Enum) Random(keys ...string) string {
	return gofakeit.RandomString(e.vals)
}
