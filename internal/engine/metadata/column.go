package metadata

import (
	"encoding/json"
	"fmt"
	"strings"

	"github.com/ymatrix-data/mxbench/pkg/mxmock"
)

type ColumnType = string

const (
	ColumnTypeTimestamp   ColumnType = "timestamp"
	ColumnTypeTimestampTZ ColumnType = "timestamptz"

	ColumnTypeText    ColumnType = "text"
	ColumnTypeVarChar ColumnType = "varchar"

	ColumnTypeInt4 = "int4"
	ColumnTypeInt8 = "int8"

	ColumnTypeJSON  ColumnType = "json"
	ColumnTypeJSONB ColumnType = "jsonb"
	ColumnTypeMXKV2_FLOAT8 ColumnType = "mxkv2_float8"
)

var supportedTSTypes = map[ColumnType]struct{}{
	ColumnTypeTimestamp:   {},
	ColumnTypeTimestampTZ: {},
}

var supportedVINTypes = map[ColumnType]struct{}{
	ColumnTypeText:    {},
	ColumnTypeVarChar: {},
	ColumnTypeInt4:    {},
	ColumnTypeInt8:    {},
}

var supportedExtTypes = map[ColumnType]struct{}{
	ColumnTypeJSON:  {},
	ColumnTypeJSONB: {},
	ColumnTypeMXKV2_FLOAT8: {},
}

type ColumnName = string

const (
	ColumnNameTS  = "ts"
	ColumnNameVIN = "vin"
	ColumnNameExt = "ext"
)

const (
	TSColumnIndex  = 0 // ts column is supposed to be the first column
	VINColumnIndex = 1 // vin column is supposed to be the second column
)

const (
	ColumnSizeVin         int64 = 32 // TODO: average vin size
	ColumnSizeTimestamp   int64 = 8
	ColumnSizeTimestampTZ int64 = 8
)

func NewColumn(n ColumnName, t ColumnType) *mxmock.Column {
	return &mxmock.Column{
		Name:     n,
		TypeName: t,
	}
}

type Columns []*mxmock.Column

// Parse columns to:
//
//		ts  timestamp
//	  , vin text
//	  , c0  float4
//	  , ...
//	  , cn float4
func (cs Columns) ToSQLStr() string {
	vars := make([]string, 0, len(cs))
	for _, c := range cs {
		sqlPiece := fmt.Sprintf("%s %s", c.Name, c.TypeName)
		if len(c.Encoding) > 0 {
			sqlPiece = fmt.Sprintf("%s ENCODING (%s)", sqlPiece, c.Encoding)
		}
		vars = append(vars, sqlPiece)
	}
	return "	" + strings.Join(vars, "\n  , ")
}

func (cs Columns) ToSelectSQLStr(tableAlias string) string {
	vars := make([]string, 0, len(cs))
	deli := ""
	if tableAlias != "" {
		deli = "."
	}
	for _, c := range cs {
		vars = append(vars, fmt.Sprintf("%s%s%s", tableAlias, deli, c.Name))
	}
	return strings.Join(vars, "\n  , ")
}

// parseColumnsDescriptions parses a string called colsDescs, into a JSON array,
// with each of the JSON in it describing a set of columns like:
// {"type": "float8", "count": 8, "comment":'{\"max\": 3.0}'}
// returns an error if colsDescs is not a legal JSON array string
func parseColumnsDescriptions(colsDescs string) (MetricsDescriptions, error) {
	if colsDescs == "" {
		return nil, nil
	}
	columnsDescriptions := make(MetricsDescriptions, 0)
	err := json.Unmarshal([]byte(colsDescs), &columnsDescriptions)
	if err != nil {
		return columnsDescriptions, err
	}
	return columnsDescriptions, nil
}

// parseColumnComment
func parseColumnComment(colComment string) (*ColumnSpec, error) {
	if colComment == "" {
		return nil, nil
	}
	var columnSpec ColumnSpec
	err := json.Unmarshal([]byte(colComment), &columnSpec)
	if err != nil {
		return nil, err
	}
	// TODO: check semantics of  columnSpec
	// for example, max should be bigger than min etc.
	return &columnSpec, nil
}

func isSupportedTSType(columnType MetricsType) bool {
	_, ok := supportedTSTypes[columnType]
	return ok
}

func isSupportedVinType(columnType MetricsType) bool {
	_, ok := supportedVINTypes[columnType]
	return ok
}

func isSupportedExtType(columnType MetricsType) bool {
	_, ok := supportedExtTypes[columnType]
	return ok
}

// separateColumnDescs separates columnDescs into 2 ColumnsDescs,
// the first one with a max total column count of maxFirstPartCount.
// example 1:
// columnDescs [{"type": float4, "count": 8}, {"type": float8, "count": 3}]
// maxFirstPartCount: 7
// returns:  [{"type": float4, "count": 7}], [{"type": float4, "count": 1}, {"type": float8, "count": 3}]
// example 2:
// columnDescs [{"type": float4, "count": 8}, {"type": float8, "count": 3}]
// maxFirstPartCount: 12
// returns:  [{"type": float4, "count": 8}, {"type": float8, "count": 3}], nil
func separateColumnDescs(columnsDescs MetricsDescriptions,
	maxFirstPartCount int64) (MetricsDescriptions, MetricsDescriptions) {
	if columnsDescs == nil {
		return nil, nil
	}

	totalColumnsCount := columnsDescs.getTotalMetricsCount()
	if totalColumnsCount <= maxFirstPartCount {
		return columnsDescs, nil
	}
	// find the columns description that crosses the border
	var metricsCount int64
	firstPartColumnsDescs, secondPartColumnsDescs := make(MetricsDescriptions, 0), make(MetricsDescriptions, 0)
	var separated bool
	for _, columnsDesc := range columnsDescs {
		// columnsDesc can entirely fit into the first part
		if metricsCount+columnsDesc.Count <= maxFirstPartCount {
			metricsCount += columnsDesc.Count
			firstPartColumnsDescs = append(firstPartColumnsDescs, columnsDesc)
			continue
		}
		if !separated {
			// The first time that columnsDesc should fall into the second part,
			// and it may trigger a separation
			separated = true
			firstPartCount := maxFirstPartCount - metricsCount
			if firstPartCount != 0 {
				// really need to separate columnsDesc
				metricsCount += firstPartCount
				// copy columnsDesc
				extDesc := *columnsDesc
				// separate columnsDesc through modifying columnsDesc and its copy's "Count" fields
				columnsDesc.Count = firstPartCount
				extDesc.Count = extDesc.Count - firstPartCount
				firstPartColumnsDescs = append(firstPartColumnsDescs, columnsDesc)
				secondPartColumnsDescs = append(secondPartColumnsDescs, &extDesc)
				continue
			}
		}
		// columnsDesc entirely falls in the secondPart
		secondPartColumnsDescs = append(secondPartColumnsDescs, columnsDesc)
	}
	return firstPartColumnsDescs, secondPartColumnsDescs
}

// NewColumnsFromColumnsDescriptions creates a slice of columns accord to columnDescs
func NewColumnsFromColumnsDescriptions(columnDescs MetricsDescriptions) (Columns, ColumnSpecs) {
	columns := make(Columns, 0)
	columnSpecs := make(ColumnSpecs, 0)
	for cdI, columnDesc := range columnDescs {
		for i := int64(0); i < columnDesc.Count; i++ {
			columns = append(columns, &mxmock.Column{
				Name:     fmt.Sprintf("c%d_%s_%d", cdI, columnDesc.MetricsType, i),
				TypeName: columnDesc.MetricsType,
			})
			columnSpecs = append(columnSpecs, &columnDesc.Spec)
		}
	}
	return columns, columnSpecs
}

// NewExtColumnFromColumnsDescriptions creates a column with the name n and type t.
// For it will be used as the ext column,
// a specification to indicate its usage and included metrics is to be properly set.
func NewExtColumnFromColumnsDescriptions(n ColumnName, t ColumnType, columnsDescs MetricsDescriptions) (*mxmock.Column, *ColumnSpec, error) {
	return &mxmock.Column{
			Name:     n,
			TypeName: t,
		}, &ColumnSpec{
			IsExt:               true,
			ColumnsDescriptions: columnsDescs,
		}, nil
}

type ColumnSpec struct {
	IsExt               bool                `json:"is-ext"`
	ColumnsDescriptions MetricsDescriptions `json:"columns-descriptions"`

	Name          string  `json:"name"`
	Min           float64 `json:"min"`
	Max           float64 `json:"max"`
	IsRounded     bool    `json:"is-rounded"`
	DecimalPlaces uint    `json:"decimal-places"`
}

type ColumnSpecs []*ColumnSpec
