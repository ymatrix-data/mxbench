package typ

import (
	"fmt"
	"math"
	"math/rand"
	"sync"

	"github.com/ymatrix-data/mxbench/internal/engine/metadata"
	"github.com/ymatrix-data/mxbench/pkg/mxmock"
)

type Float8 struct {
	mxmock.BaseType
	columnSpec *metadata.ColumnSpec

	valueRanges *mxmock.ValueRange
	mu          sync.RWMutex
}

func GetNewFloat8(table *metadata.Table) func(string) mxmock.Type {
	return func(colName string) mxmock.Type {
		f8 := &Float8{
			BaseType: mxmock.NewBaseType(colName),
		}
		for colInd, col := range table.Columns {
			if col.Name == colName {
				f8.columnSpec = table.ColumnSpecs[colInd]
				break
			}
		}
		return f8
	}
}

func (f8 *Float8) Random(keys ...string) string {
	for _, key := range keys {
		if key != f8.GetColName() {
			continue
		}

		return f8.generateValue()

	}
	return ""
}

func (f8 *Float8) generateValue() string {
	var value float64

	//TODO: performance issue
	if f8.columnSpec == nil || (int(f8.columnSpec.Min) == 0 && int(f8.columnSpec.Max) == 0) {
		value = rand.Float64()
	} else {
		switch f8.columnSpec.Name {
		case _NULL:
			return ""
		default:
		}

		value = rand.Float64()*(f8.columnSpec.Max-f8.columnSpec.Min) + f8.columnSpec.Min
		if f8.columnSpec.IsRounded {
			return fmt.Sprintf("%f", roundFloat(value, f8.columnSpec.DecimalPlaces))
		}
	}

	f8.updateValueRange(value)

	return fmt.Sprintf("%f", value)
}

func (f8 *Float8) ValueRange() map[string]*mxmock.ValueRange {
	f8.mu.RLock()
	defer f8.mu.RUnlock()

	return map[string]*mxmock.ValueRange{
		"float8": f8.valueRanges,
	}
}

func roundFloat(val float64, precision uint) float64 {
	ratio := math.Pow(10, float64(precision))
	return math.Round(val*ratio) / ratio
}

func (f8 *Float8) updateValueRange(value float64) {
	f8.mu.Lock()
	defer f8.mu.Unlock()

	if f8.valueRanges == nil {
		f8.valueRanges = &mxmock.ValueRange{
			Min: value,
			Max: value,
		}
	} else {
		if value < f8.valueRanges.Min.(float64) {
			f8.valueRanges.Min = value
		}
		if value > f8.valueRanges.Max.(float64) {
			f8.valueRanges.Max = value
		}
	}
}
