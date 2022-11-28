package mxmock

import (
	"fmt"
	"math/rand"
	"strings"
	"time"

	"github.com/brianvoe/gofakeit/v6"
	"github.com/jmoiron/sqlx"
)

/**
 * Mock data for YMatrix
 */
type MXMocker struct {
	conn                    *sqlx.DB
	columns                 []*Column
	keyMap                  map[string]bool
	RowCh                   chan []string
	fakeAutoIncrementColumn bool
}

func NewMXMockerFromColumns(columns []*Column) (*MXMocker, error) {
	faker := gofakeit.New(time.Now().UnixNano())
	gofakeit.SetGlobalFaker(faker)

	mocker := &MXMocker{
		columns: columns,
		RowCh:   make(chan []string, 1),
	}

	err := mocker.init()
	return mocker, err
}

func NewMXMocker(conn *sqlx.DB, schema, table string) (*MXMocker, error) {
	columns, err := NewColumnsFromDB(conn, schema, table)
	if err != nil {
		return nil, err
	}

	return NewMXMockerFromColumns(columns)
}

func (m *MXMocker) init() error {
	if m.keyMap == nil {
		m.keyMap = map[string]bool{}
	}
	for _, c := range m.columns {
		err := c.initTyp(m.conn)
		if err != nil {
			return err
		}
		keys := c.keys()
		for _, key := range keys {
			m.keyMap[key] = true
		}
	}
	return nil
}

func (m *MXMocker) FakeAutoIncrementColumn() {
	m.fakeAutoIncrementColumn = true
}

func (m *MXMocker) IgnoreAutoIncrementColumn() {
	m.fakeAutoIncrementColumn = false
}

func (m *MXMocker) ExcludeColumn(names ...string) {
	nMap := map[string]bool{}
	for _, name := range names {
		nMap[name] = true
	}
	ret := []*Column{}
	// need to record excluded columns' indexes
	for _, column := range m.columns {
		if nMap[column.Name] {
			continue
		}
		ret = append(ret, column)
	}
	m.columns = ret

	// also need to exclude them from keyMap
	for key := range nMap {
		delete(m.keyMap, key)
	}
}

func NewColumnsFromDB(conn *sqlx.DB, schema, table string) ([]*Column, error) {
	rows, err := conn.Query(_SELECT_COLUMN, schema, table)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var columns []*Column
	for rows.Next() {
		var item Column
		err = rows.Scan(&item.Name, &item.TypeName, &item.TypeDesc, &item.DefVal, &item.Comment)
		if err != nil {
			return nil, err
		}
		columns = append(columns, &item)
	}

	if err = rows.Err(); err != nil {
		return nil, err
	}
	return columns, nil
}

func (m *MXMocker) Mock(length int) [][]string {
	rows := [][]string{}
	for i := 0; i < length; i++ {
		row := m.MockRow()
		rows = append(rows, row)
	}
	return rows
}

func (m *MXMocker) AsyncMock(length int) {
	for i := 0; i < length; i++ {
		row := m.MockRow()
		m.RowCh <- row
	}
}

// Mock one row for current table
func (m *MXMocker) MockRow() []string {
	row := []string{}
	for _, c := range m.columns {
		// Ignore auto increment column
		if strings.HasPrefix(c.DefVal, "nextval") {
			if m.fakeAutoIncrementColumn {
				panic("To be implemented")
			} else {
				continue
			}
		}
		v := c.mock()
		row = append(row, v)
	}
	return row
}

// Mock one row belong a batch for current table
func (m *MXMocker) MockBatchRow(keys ...string) []string {
	row := []string{}
	for _, c := range m.columns {
		// Ignore auto increment column
		if strings.HasPrefix(c.DefVal, "nextval") {
			if m.fakeAutoIncrementColumn {
				panic("To be implemented")
			} else {
				continue
			}
		}
		v := c.mockBatch(keys...)
		row = append(row, v)
	}
	return row
}

// Mock a batch of rows with ${lines} lines and every row has ${values} values
func (m *MXMocker) MockBatch(lines, values int) ([][]string, error) {
	if values > len(m.keyMap) {
		return nil, fmt.Errorf("batch size can't be bigger than keys number")
	}
	if lines*values > len(m.keyMap) {
		return nil, fmt.Errorf("could't generate those lines")
	}
	var keys = [][]string{}
	var flag int
	var rowKeys = []string{}
	for key := range m.keyMap {
		if len(keys) >= lines {
			break
		}
		rowKeys = append(rowKeys, key)
		if flag >= values-1 {
			keys = append(keys, rowKeys)
			rowKeys = []string{}
			flag = 0
		} else {
			flag++
		}
	}
	result := [][]string{}
	for _, rowKeys := range keys {
		row := m.MockBatchRow(rowKeys...)
		result = append(result, row)
	}
	return result, nil
}

// Mock a batch of rows with ${lines} lines, with ${totalValues} non-empty values in total,
// with no overlapping non-empty values among lines
func (m *MXMocker) MockBatchWithTotalValues(lines, totalValues int) ([][]string, error) {
	if totalValues > len(m.keyMap) {
		return nil, fmt.Errorf("non-empty values can't be bigger than keys number: %d > %d ", totalValues, len(m.keyMap))
	}

	// Generate sampled column indexes/names
	keyNames := make([]string, 0, len(m.keyMap))
	for keyName := range m.keyMap {
		keyNames = append(keyNames, keyName)
	}
	rand.Shuffle(len(keyNames), func(i, j int) { keyNames[i], keyNames[j] = keyNames[j], keyNames[i] })
	keyNames = keyNames[:totalValues]

	result := [][]string{}

	// if lines is bigger than totalValues
	// then the leading ${totalValues} lines only get 1 non-empty value
	// the rest of lines have no non-empty value
	if lines > totalValues {
		for _, keyName := range keyNames {
			row := m.MockBatchRow(keyName)
			result = append(result, row)
		}
		for lineNum := 0; lineNum < lines-totalValues; lineNum++ {
			result = append(result, m.MockBatchRow())
		}
		return result, nil
	}

	leadingRowsValues := totalValues / lines
	for lineNum := 0; lineNum < lines-1; lineNum++ {
		row := m.MockBatchRow(keyNames[lineNum*leadingRowsValues : lineNum*leadingRowsValues+leadingRowsValues]...)
		result = append(result, row)
	}
	lastRow := m.MockBatchRow(keyNames[(lines-1)*leadingRowsValues:]...)
	result = append(result, lastRow)

	return result, nil
}
