package typ

import (
	"bytes"
	"encoding/json"
	"fmt"
	"math/rand"
	"strings"

	"github.com/brianvoe/gofakeit/v6"

	"github.com/ymatrix-data/mxbench/internal/engine/metadata"
	"github.com/ymatrix-data/mxbench/internal/util/log"
	"github.com/ymatrix-data/mxbench/pkg/mxmock"
)

type JSON struct {
	metricsType    metadata.MetricsType
	metricsCount   int64
	columnName     string
	isCommentedExt bool
	mxmock.BaseType
	keys       []string
	kdMap      map[string]*metadata.MetricsDescription
	columnSpec *metadata.ColumnSpec

	Name string `json:"name"`
	Age  int    `json:"number" fake:"{number:1,100}"`

	valueRanges map[string]*mxmock.ValueRange
}

func GetNewJSON(table *metadata.Table) func(string) mxmock.Type {
	return func(colName string) mxmock.Type {
		var keys []string
		var kdMap map[string]*metadata.MetricsDescription
		var columnSpec *metadata.ColumnSpec
		for colInd, col := range table.Columns {
			if col.Name == colName {
				columnSpec = table.ColumnSpecs[colInd]
				break
			}
		}
		if colName == table.ColumnNameExt && len(table.ColumnsDescsExt) > 0 {
			// this means this is for ext col and it is explicitly commented to identify flatten metrics
			keys = make([]string, 0)
			kdMap = map[string]*metadata.MetricsDescription{}
			for cdI, colsDesc := range table.ColumnsDescsExt {
				for i := int64(0); i < colsDesc.Count; i++ {
					key := fmt.Sprintf("k%d_%s_%d", cdI, colsDesc.MetricsType, i)
					keys = append(keys, key)
					kdMap[key] = colsDesc
				}
			}
		}
		return &JSON{
			metricsType:    table.JSONMetricsCandidateType,
			metricsCount:   table.JSONMetricsCount,
			columnName:     colName,
			isCommentedExt: colName == table.ColumnNameExt,
			BaseType:       mxmock.NewBaseType(colName),
			keys:           keys,
			kdMap:          kdMap,
			columnSpec:     columnSpec,
			valueRanges:    make(map[string]*mxmock.ValueRange),
		}
	}
}

func (j *JSON) Random(keys ...string) string {
	// if it not for ext column to accommodate a lot of non-json metrics
	// i.e. it is just an average json column

	//log.Info("isCommentedExt: %v, json random colName: %s, keys: %+v", j.isCommentedExt, j.columnName, keys)
	if !j.isCommentedExt {
		for _, key := range keys {
			if key != j.columnName {
				continue
			}
			_ = gofakeit.Struct(j)
			b, _ := json.Marshal(j)
			return string(b)
		}
		return "{}"
	}

	// if it is ext column:
	// if it does not have column description, then follow metricsType and metricsNum configuration
	if j.columnSpec == nil || len(j.columnSpec.ColumnsDescriptions) == 0 {
		buff := bytes.NewBuffer(nil)
		buff.WriteString("\"{")
		vs := []string{}
		keyMap := map[string]bool{}
		for _, key := range j.Keys() {
			keyMap[key] = true
		}
		for _, key := range keys {
			if !keyMap[key] {
				continue
			}

			value := j.generateValue(j.metricsType, nil, nil)
			//TODO: performance issue
			switch j.metricsType {
			case metadata.MetricsTypeInt4:
				vs = append(vs, fmt.Sprintf("\"\"%s\"\":%d", key, value))
			case metadata.MetricsTypeInt8:
				vs = append(vs, fmt.Sprintf("\"\"%s\"\":%d", key, value))
			case metadata.MetricsTypeFloat4:
				vs = append(vs, fmt.Sprintf("\"\"%s\"\":%f", key, value))
			case metadata.MetricsTypeFloat8:
				vs = append(vs, fmt.Sprintf("\"\"%s\"\":%f", key, value))
			}
		}
		buff.WriteString(strings.Join(vs, ","))
		buff.WriteString("}\"")
		return buff.String()
	}

	// else follow column descriptions
	if len(j.kdMap) == 0 {
		log.Warn("cannot find any columns descriptions for ext column")
		return "{}"
	}
	buff := bytes.NewBuffer(nil)
	buff.WriteString("\"{")
	vs := []string{}

	for _, key := range keys {
		columnsDesc, ok := j.kdMap[key]
		if !ok {
			continue
		}

		value := j.generateValue(columnsDesc.MetricsType, nil, nil)
		//TODO: performance issue, support other types
		if int(columnsDesc.Spec.Min) == 0 && int(columnsDesc.Spec.Max) == 0 {
			switch columnsDesc.MetricsType {
			case metadata.MetricsTypeInt4:
				vs = append(vs, fmt.Sprintf("\"\"%s\"\":%d", key, value))
			case metadata.MetricsTypeInt8:
				vs = append(vs, fmt.Sprintf("\"\"%s\"\":%d", key, value))
			case metadata.MetricsTypeFloat4:
				vs = append(vs, fmt.Sprintf("\"\"%s\"\":%f", key, value))
			case metadata.MetricsTypeFloat8:
				vs = append(vs, fmt.Sprintf("\"\"%s\"\":%f", key, value))
			}
		} else {
			value := j.generateValue(columnsDesc.MetricsType, columnsDesc.Spec.Min, columnsDesc.Spec.Max)
			switch columnsDesc.MetricsType {
			case metadata.MetricsTypeInt4:
				vs = append(vs, fmt.Sprintf("\"\"%s\"\":%d", key, value))
			case metadata.MetricsTypeInt8:
				vs = append(vs, fmt.Sprintf("\"\"%s\"\":%d", key, value))
			case metadata.MetricsTypeFloat4:
				vs = append(vs, fmt.Sprintf("\"\"%s\"\":%f", key, value))
			case metadata.MetricsTypeFloat8:
				vs = append(vs, fmt.Sprintf("\"\"%s\"\":%f", key, value))
			}
		}

	}
	buff.WriteString(strings.Join(vs, ","))
	buff.WriteString("}\"")
	return buff.String()
}

func (j *JSON) generateValue(tp metadata.MetricsType, min, max interface{}) interface{} {
	var value interface{}
	//var valueStr string

	if min == nil && max == nil {
		// generate random value
		switch tp {
		case metadata.MetricsTypeInt4:
			value = rand.Int31()
			//valueStr = fmt.Sprintf("%d", value)
		case metadata.MetricsTypeInt8:
			value = rand.Int63()
			//valueStr = fmt.Sprintf("%d", value)
		case metadata.MetricsTypeFloat4:
			value = rand.Float32()
			//valueStr = fmt.Sprintf("%f", value)
		case metadata.MetricsTypeFloat8:
			value = rand.Float64()
			//valueStr = fmt.Sprintf("%f", value)
		}
	} else {
		// generate random value within range
		switch tp {
		case metadata.MetricsTypeInt4:
			value = rand.Int31n(max.(int32)-min.(int32)) + min.(int32)
			//valueStr = fmt.Sprintf("%d", value)
		case metadata.MetricsTypeInt8:
			value = rand.Int63n(max.(int64)-min.(int64)) + min.(int64)
			//valueStr = fmt.Sprintf("%d", value)
		case metadata.MetricsTypeFloat4:
			value = rand.Float32()*(max.(float32)-min.(float32)) + min.(float32)
			//valueStr = fmt.Sprintf("%f", value)
		case metadata.MetricsTypeFloat8:
			value = rand.Float64()*(max.(float64)-min.(float64)) + min.(float64)
			//valueStr = fmt.Sprintf("%f", value)
		}
	}

	if value == nil {
		// should not happen
		return value
	}

	j.updateRange(tp, value)

	return value
}

// update value range based on type
func (j *JSON) updateRange(tp metadata.MetricsType, value interface{}) {
	if _, ok := j.valueRanges[tp]; !ok {
		j.valueRanges[tp] = &mxmock.ValueRange{
			Min: value,
			Max: value,
		}
	} else {
		var updateMin, updateMax bool
		switch tp {
		case metadata.MetricsTypeInt4:
			updateMin = value.(int32) < j.valueRanges[tp].Min.(int32)
			updateMax = value.(int32) > j.valueRanges[tp].Max.(int32)
		case metadata.MetricsTypeInt8:
			updateMin = value.(int64) < j.valueRanges[tp].Min.(int64)
			updateMax = value.(int64) > j.valueRanges[tp].Max.(int64)
		case metadata.MetricsTypeFloat4:
			updateMin = value.(float32) < j.valueRanges[tp].Min.(float32)
			updateMax = value.(float32) > j.valueRanges[tp].Max.(float32)
		case metadata.MetricsTypeFloat8:
			updateMin = value.(float64) < j.valueRanges[tp].Min.(float64)
			updateMax = value.(float64) > j.valueRanges[tp].Max.(float64)
		}

		if updateMin {
			j.valueRanges[tp].Min = value
		}
		if updateMax {
			j.valueRanges[tp].Max = value
		}
	}
}

func (j *JSON) Keys() []string {
	if len(j.keys) > 0 {
		return j.keys
	}
	// if it not for ext column to accommodate a lot of non-json metrics
	// i.e. it is just an average json column
	if !j.isCommentedExt {
		return []string{j.columnName}
	}
	// if it is ext column:
	// if it does not have column description, then follow metricsType and metricsNum configuration
	if j.columnSpec == nil || len(j.columnSpec.ColumnsDescriptions) == 0 {
		keys := make([]string, j.metricsCount)
		for i := int64(0); i < j.metricsCount; i++ {
			keys[i] = fmt.Sprintf("k%d_%s", i, j.metricsType)
		}
		return keys
	}

	//  else follow column descriptions
	keys := make([]string, 0)
	for cdI, colsDesc := range j.columnSpec.ColumnsDescriptions {
		for i := int64(0); i < colsDesc.Count; i++ {
			keys = append(keys, fmt.Sprintf("k%d_%s_%d", cdI, colsDesc.MetricsType, i))
		}
	}
	return keys
}

func (j *JSON) ValueRange() map[string]*mxmock.ValueRange {
	return j.valueRanges
}
