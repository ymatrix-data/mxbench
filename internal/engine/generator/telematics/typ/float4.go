package typ

import (
	"fmt"
	"math/rand"

	"github.com/ymatrix-data/mxbench/internal/engine/metadata"
	"github.com/ymatrix-data/mxbench/pkg/mxmock"
)

type Float4 struct {
	mxmock.BaseType
	columnSpec *metadata.ColumnSpec
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
		//TODO: performance issue
		if f4.columnSpec == nil || (int(f4.columnSpec.Min) == 0 && int(f4.columnSpec.Max) == 0) {
			return fmt.Sprintf("%f", rand.Float32())
		}
		switch f4.columnSpec.Name {
		case _NULL:
			return ""
		default:
		}
		value64 := rand.Float64()*(f4.columnSpec.Max-f4.columnSpec.Min) + f4.columnSpec.Min
		if f4.columnSpec.IsRounded {
			return fmt.Sprintf("%f", roundFloat(value64, f4.columnSpec.DecimalPlaces))
		}
		return fmt.Sprintf("%f", float32(value64))

	}
	return ""
}

func (f4 *Float4) ValueRange() map[string]*mxmock.ValueRange {
	return nil
}
