package typ

import (
	"fmt"
	"math/rand"
	"sync"

	"github.com/ymatrix-data/mxbench/internal/engine/metadata"
	"github.com/ymatrix-data/mxbench/pkg/mxmock"
)

type Int8 struct {
	mxmock.BaseType
	columnSpec *metadata.ColumnSpec

	valueRanges *mxmock.ValueRange
	mu          sync.RWMutex
}

func GetNewInt8(table *metadata.Table) func(string) mxmock.Type {
	return func(colName string) mxmock.Type {
		i8 := &Int8{
			BaseType: mxmock.NewBaseType(colName),
		}
		for colInd, col := range table.Columns {
			if col.Name == colName {
				i8.columnSpec = table.ColumnSpecs[colInd]
				break
			}
		}
		return i8
	}
}

func (i8 *Int8) Random(keys ...string) string {
	for _, key := range keys {
		if key != i8.GetColName() {
			continue
		}
		return i8.generateValue()
	}
	return ""
}

func (i8 *Int8) ValueRange() map[string]*mxmock.ValueRange {
	i8.mu.RLock()
	defer i8.mu.RUnlock()

	return map[string]*mxmock.ValueRange{
		"int8": i8.valueRanges,
	}
}

func (i8 *Int8) generateValue() string {
	var value int64
	//TODO: performance issue
	//tolerate the case that the user didn't set comment on the column at all
	if i8.columnSpec == nil || (int64(i8.columnSpec.Min) == 0 && int64(i8.columnSpec.Max) == 0) {
		value = rand.Int63()
	} else {
		value = rand.Int63n(int64(i8.columnSpec.Max) - int64(i8.columnSpec.Min) + int64(i8.columnSpec.Min))
	}

	i8.updateValueRange(value)

	return fmt.Sprintf("%d", value)
}

func (i8 *Int8) updateValueRange(value int64) {
	i8.mu.Lock()
	defer i8.mu.Unlock()

	if i8.valueRanges == nil {
		i8.valueRanges = &mxmock.ValueRange{
			Min: value,
			Max: value,
		}
	} else {
		if value < i8.valueRanges.Min.(int64) {
			i8.valueRanges.Min = value
		}
		if value > i8.valueRanges.Max.(int64) {
			i8.valueRanges.Max = value
		}
	}
}
