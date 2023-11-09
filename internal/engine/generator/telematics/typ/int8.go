package typ

import (
	"fmt"
	"math/rand"

	"github.com/ymatrix-data/mxbench/internal/engine/metadata"
	"github.com/ymatrix-data/mxbench/pkg/mxmock"
)

type Int8 struct {
	mxmock.BaseType
	columnSpec *metadata.ColumnSpec
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
		//TODO: performance issue
		//tolerate the case that the user didn't set comment on the column at all
		if i8.columnSpec == nil || (int64(i8.columnSpec.Min) == 0 && int64(i8.columnSpec.Max) == 0) {
			return fmt.Sprintf("%d", rand.Int63())
		}
		return fmt.Sprintf("%d", rand.Int63n(int64(i8.columnSpec.Max)-int64(i8.columnSpec.Min)+int64(i8.columnSpec.Min)))
	}
	return ""
}

func (i8 *Int8) ValueRange() map[string]*mxmock.ValueRange {
	return nil
}
