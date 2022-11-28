package mxmock

import (
	"fmt"

	"github.com/jmoiron/sqlx"
)

type Column struct {
	Name     string `db:"attname"`
	TypeName string `db:"typname"`
	TypeDesc string `db:"typdesc"`
	DefVal   string `db:"defval"`
	Comment  string `db:"comment"`
	Encoding string `db:"encoding"`
	typ      Type
}

func (c *Column) initTyp(conn *sqlx.DB) error {
	var err error
	var enum Type
	initFunc := func() {
		if f, ok := TypMap[c.TypeName]; ok {
			t := f(c.Name)
			t.Parse(c.TypeDesc)
			c.typ = t
			return
		}
		if conn == nil {
			err = fmt.Errorf("unknown type:%s", c.TypeName)
			return
		}
		// If type not defined in TypMap, means the type is enum
		enum, err = NewEnum(c.TypeName, conn)
		if err != nil {
			return
		}
		c.typ = enum
	}
	initFunc()
	return err
}

func (c *Column) mustGetTyp() Type {
	if c.typ == nil {
		panic(fmt.Sprintf("[%s] must init type", c.Name))
	}
	return c.typ
}

func (c *Column) keys() []string {
	return c.mustGetTyp().Keys()
}

func (c *Column) mock() string {
	return c.mustGetTyp().Random(c.Name)
}

// With the input keys, just generate the keys belong current type.
func (c *Column) mockBatch(keys ...string) string {
	return c.mustGetTyp().Random(keys...)
}

func (cs *Column) WithEncoding(enc string) *Column {
	cs.Encoding = enc
	return cs
}
