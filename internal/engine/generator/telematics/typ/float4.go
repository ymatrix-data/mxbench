package typ

import (
	"fmt"
	"math/rand"
	"sync"

	"github.com/ymatrix-data/mxbench/internal/engine/metadata"
	"github.com/ymatrix-data/mxbench/pkg/mxmock"
)

type Float4 struct {
	mxmock.BaseType
	columnSpec *metadata.ColumnSpec

	valueRanges *mxmock.ValueRange
	mu          sync.RWMutex
}

func GetNewFloat4(table *metadata.Table) func(string) mxmock.Type {
	return func(colName string) mxmock.Type {
		f4 := &Float4{
			BaseType: mxmock.NewBaseType(colName),
		}
		for colInd, col := range table.Columns {
			if col.Name == colName {
				f4.columnSpec = table.ColumnSpecs[colInd]
				break
			}
		}
		return f4
	}
}

func (f4 *Float4) Random(keys ...string) string {
	for _, key := range keys {
		if key != f4.GetColName() {
			continue
		}

		return f4.generateValue()
	}
	return ""
}

func (f4 *Float4) generateValue() string {
	var value float32

	//TODO: performance issue
	if f4.columnSpec == nil || (int(f4.columnSpec.Min) == 0 && int(f4.columnSpec.Max) == 0) {
		value = rand.Float32()
	} else {
		switch f4.columnSpec.Name {
		case _NULL:
			return ""
		default:
		}

		value64 := rand.Float64()*(f4.columnSpec.Max-f4.columnSpec.Min) + f4.columnSpec.Min
		if f4.columnSpec.IsRounded {
			return fmt.Sprintf("%f", roundFloat(value64, f4.columnSpec.DecimalPlaces))
		}
		value = float32(value64)
	}

	f4.updateValueRange(value)

	return fmt.Sprintf("%f", float32(value))
}

func (f4 *Float4) ValueRange() map[string]*mxmock.ValueRange {
	f4.mu.RLock()
	defer f4.mu.RUnlock()

	return map[string]*mxmock.ValueRange{
		"float4": f4.valueRanges,
	}
}

func (f4 *Float4) updateValueRange(value float32) {
	f4.mu.Lock()
	defer f4.mu.Unlock()

	if f4.valueRanges == nil {
		f4.valueRanges = &mxmock.ValueRange{
			Min: value,
			Max: value,
		}
	} else {
		if value < f4.valueRanges.Min.(float32) {
			f4.valueRanges.Min = value
		}
		if value > f4.valueRanges.Max.(float32) {
			f4.valueRanges.Max = value
		}
	}

	return
}
