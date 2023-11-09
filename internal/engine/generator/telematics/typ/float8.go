package typ

import (
	"fmt"
	"math"
	"math/rand"

	"github.com/ymatrix-data/mxbench/internal/engine/metadata"
	"github.com/ymatrix-data/mxbench/pkg/mxmock"
)

type Float8 struct {
	mxmock.BaseType
	columnSpec *metadata.ColumnSpec
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
	//fmt.Printf("float8 random, keys, %+v, column: %s\n, stack: %s", keys, f8.GetColName(), debug.Stack())

	for _, key := range keys {
		if key != f8.GetColName() {
			continue
		}
		//TODO: performance issue
		if f8.columnSpec == nil || (int(f8.columnSpec.Min) == 0 && int(f8.columnSpec.Max) == 0) {
			return fmt.Sprintf("%f", rand.Float64())
		}
		switch f8.columnSpec.Name {
		case _NULL:
			return ""
		default:
		}
		value := rand.Float64()*(f8.columnSpec.Max-f8.columnSpec.Min) + f8.columnSpec.Min
		if f8.columnSpec.IsRounded {
			return fmt.Sprintf("%f", roundFloat(value, f8.columnSpec.DecimalPlaces))
		}
		return fmt.Sprintf("%f", value)

	}
	return ""
}

func (f8 *Float8) ValueRange() map[string]*mxmock.ValueRange {
	return nil
}

func roundFloat(val float64, precision uint) float64 {
	ratio := math.Pow(10, float64(precision))
	return math.Round(val*ratio) / ratio
}
