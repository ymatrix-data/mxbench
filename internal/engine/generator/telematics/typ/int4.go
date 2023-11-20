package typ

import (
	"fmt"
	"math/rand"
	"sync"

	"github.com/ymatrix-data/mxbench/internal/engine/metadata"
	"github.com/ymatrix-data/mxbench/pkg/mxmock"
)

type Int4 struct {
	mxmock.BaseType
	columnSpec *metadata.ColumnSpec

	valueRanges *mxmock.ValueRange
	mu          sync.RWMutex
}

func GetNewInt4(table *metadata.Table) func(string) mxmock.Type {
	return func(colName string) mxmock.Type {
		i4 := &Int4{
			BaseType: mxmock.NewBaseType(colName),
		}
		for colInd, col := range table.Columns {
			if col.Name == colName {
				i4.columnSpec = table.ColumnSpecs[colInd]
				break
			}
		}
		return i4
	}
}

func (i4 *Int4) Random(keys ...string) string {
	for _, key := range keys {
		if key != i4.GetColName() {
			continue
		}
		return i4.generateValue()
	}
	return ""
}

func (i4 *Int4) ValueRange() map[string]*mxmock.ValueRange {
	i4.mu.RLock()
	defer i4.mu.RUnlock()

	return map[string]*mxmock.ValueRange{
		"int4": i4.valueRanges,
	}
}

func (i4 *Int4) generateValue() string {
	var value int32
	//TODO: performance issue
	//tolerate the case that the user didn't set comment on the column at all
	if i4.columnSpec == nil || (int32(i4.columnSpec.Min) == 0 && int32(i4.columnSpec.Max) == 0) {
		value = rand.Int31()
	} else {
		value = rand.Int31n(int32(i4.columnSpec.Max)-int32(i4.columnSpec.Min)) + int32(i4.columnSpec.Min)
	}

	i4.updateValueRange(value)

	return fmt.Sprintf("%d", value)
}

func (i4 *Int4) updateValueRange(value int32) {
	i4.mu.Lock()
	defer i4.mu.Unlock()

	if i4.valueRanges == nil {
		i4.valueRanges = &mxmock.ValueRange{
			Min: value,
			Max: value,
		}
	} else {
		if value < i4.valueRanges.Min.(int32) {
			i4.valueRanges.Min = value
		}
		if value > i4.valueRanges.Max.(int32) {
			i4.valueRanges.Max = value
		}
	}
}
