package typ

import (
	"fmt"
	"math/rand"

	"github.com/ymatrix-data/mxbench/internal/engine/metadata"
	"github.com/ymatrix-data/mxbench/pkg/mxmock"
)

type Int4 struct {
	mxmock.BaseType
	columnSpec *metadata.ColumnSpec
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
		//TODO: performance issue
		//tolerate the case that the user didn't set comment on the column at all
		if i4.columnSpec == nil || (int32(i4.columnSpec.Min) == 0 && int32(i4.columnSpec.Max) == 0) {
			return fmt.Sprintf("%d", rand.Int31())
		}
		return fmt.Sprintf("%d", rand.Int31n(int32(i4.columnSpec.Max)-int32(i4.columnSpec.Min))+int32(i4.columnSpec.Min))
	}
	return ""
}

func (i4 *Int4) ValueRange() map[string]*mxmock.ValueRange {
	return nil
}
