package typ

import (
	"github.com/ymatrix-data/mxbench/internal/engine/metadata"
	"github.com/ymatrix-data/mxbench/pkg/mxmock"
	"github.com/ymatrix-data/mxbench/internal/util"
	"math"
	"fmt"
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
	if vc.columnSpec != nil && vc.columnSpec.Fixed {
		return vc.columnSpec.FixedValue
	} else if vc.columnSpec != nil && vc.columnSpec.ForceNumeric && vc.columnSpec.NumericLength > 0 {
		maxNumber := int(math.Pow10(vc.columnSpec.NumericLength)) - 1 // Calculate the maximum number
		if vc.columnSpec.Max > 0 && int(vc.columnSpec.Max) < maxNumber {
			maxNumber = int(vc.columnSpec.Max)
		}

        randomNumber := util.Intn(maxNumber + 1) // Generate a random number between 0 and maxNumber
        format := fmt.Sprintf("%%0%dd", vc.columnSpec.NumericLength) // Create a format string
        return fmt.Sprintf(format, randomNumber) // Format the number as a vc.columnSpec.NumericLength-digit string, padding with zeros if necessary
	} else if vc.columnSpec == nil || !mxmock.IsValidTemplateName(vc.columnSpec.Name) {
		return vc.VarChar.Random(keys...)
	}

	return mxmock.GenerateValByTemplate(vc.columnSpec.Name)
}
