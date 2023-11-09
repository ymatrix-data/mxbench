package typ

import (
	"github.com/ymatrix-data/mxbench/internal/engine/metadata"
	"github.com/ymatrix-data/mxbench/pkg/mxmock"
)

const (
	_NULL = "null"
)

func Init(table *metadata.Table) {
	//
	mxmock.TypMap["float4"] = GetNewFloat4(table)
	mxmock.TypMap["float8"] = GetNewFloat8(table)

	mxmock.TypMap["int4"] = GetNewInt4(table)
	mxmock.TypMap["int8"] = GetNewInt8(table)

	mxmock.TypMap["varchar"] = GetNewVarChar(table)
	mxmock.TypMap["json"] = GetNewJSON(table)
	mxmock.TypMap["jsonb"] = mxmock.TypMap["json"]
}
