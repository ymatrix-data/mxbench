package typ

import (
	"github.com/ymatrix-data/mxbench/internal/engine/metadata"
	"github.com/ymatrix-data/mxbench/pkg/mxmock"
)

type VarChar struct {
	mxmock.VarChar
	columnSpec *metadata.ColumnSpec
}

func GetNewVarChar(table *metadata.Table) func(string) mxmock.Type {
	return func(colName string) mxmock.Type {
		vc := &VarChar{
			VarChar: mxmock.VarChar{
				BaseType: mxmock.NewBaseType(colName),
			},
		}
		for colInd, col := range table.Columns {
			if col.Name == colName {
				vc.columnSpec = table.ColumnSpecs[colInd]
				break
			}
		}
		return vc
	}
}

func (vc *VarChar) Random(keys ...string) string {
	if vc.columnSpec == nil || !mxmock.IsValidTemplateName(vc.columnSpec.Name) {
		return vc.VarChar.Random(keys...)
	}

	return mxmock.GenerateValByTemplate(vc.columnSpec.Name)
}
