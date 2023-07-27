package metadata

import (
	"fmt"

	"github.com/lib/pq"
)

const (
	_CREATE_MARS2_INDEX_FMT = `
CREATE INDEX IF NOT EXISTS %s ON %s
USING %s(
	%s
  , %s
)
WITH(uniquemode=%v);
`

	_CREATE_MARS3_INDEX_FMT = `
CREATE INDEX IF NOT EXISTS %s ON %s
USING %s(
	%s
  , %s
);
`
)

type IndexType = string

const (
	IndexMars2BTree IndexType = "mars2_btree"
	IndexMars3BTree IndexType = "mars3_brin"
)

type Index interface {
	GetCreateIndexSQLStr() string
}

type Indexes []Index

type Mars2BTree struct {
	name       string
	Table      *Table
	UniqueMode bool

	// TODO: time_bucket related settings are deprecated in mars2_btree
	TimeBucketInSecond int
	TimestampColumn    ColumnName

	TagColumn ColumnName
}

func NewMars2BTree(table *Table, timeBucketInSecond int, hasUniqueConstraints bool) Index {
	return &Mars2BTree{
		name:               "idx_" + table.name,
		Table:              table,
		TimeBucketInSecond: timeBucketInSecond,
		TimestampColumn:    ColumnNameTS,
		TagColumn:          ColumnNameVIN,
		UniqueMode:         hasUniqueConstraints,
	}
}
func (s *Mars2BTree) Identifier() string {
	return pq.QuoteIdentifier(s.name)
}
func (s *Mars2BTree) GetCreateIndexSQLStr() string {
	return fmt.Sprintf(
		_CREATE_MARS2_INDEX_FMT,
		s.Identifier(),
		s.Table.Identifier(),
		IndexMars2BTree,
		s.TagColumn,
		s.TimestampColumn,
		s.UniqueMode,
	)
}

type Mars3BTree struct {
	name            string
	Table           *Table
	TimestampColumn ColumnName
	TagColumn       ColumnName
}

func NewMars3BTree(table *Table) Index {
	return &Mars3BTree{
		name:            "idx_" + table.name,
		Table:           table,
		TimestampColumn: ColumnNameTS,
		TagColumn:       ColumnNameVIN,
	}
}
func (s *Mars3BTree) Identifier() string {
	return pq.QuoteIdentifier(s.name)
}
func (s *Mars3BTree) GetCreateIndexSQLStr() string {
	return fmt.Sprintf(
		_CREATE_MARS3_INDEX_FMT,
		s.Identifier(),
		s.Table.Identifier(),
		IndexMars3BTree,
		s.TagColumn,
		s.TimestampColumn,
	)
}
