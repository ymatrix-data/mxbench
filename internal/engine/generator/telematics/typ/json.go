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
		}
	}
}

func (j *JSON) Random(keys ...string) string {
	// if it not for ext column to accommodate a lot of non-json metrics
	// i.e. it is just an average json column
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
			//TODO: performance issue
			switch j.metricsType {
			case metadata.MetricsTypeInt4:
				vs = append(vs, fmt.Sprintf("\"\"%s\"\":%d", key, rand.Int31()))
			case metadata.MetricsTypeInt8:
				vs = append(vs, fmt.Sprintf("\"\"%s\"\":%d", key, rand.Int63()))
			case metadata.MetricsTypeFloat4:
				vs = append(vs, fmt.Sprintf("\"\"%s\"\":%f", key, rand.Float32()))
			case metadata.MetricsTypeFloat8:
				vs = append(vs, fmt.Sprintf("\"\"%s\"\":%f", key, rand.Float64()))

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
		//TODO: performance issue, support other types
		if int(columnsDesc.Spec.Min) == 0 && int(columnsDesc.Spec.Max) == 0 {
			switch columnsDesc.MetricsType {
			case metadata.MetricsTypeInt4:
				vs = append(vs, fmt.Sprintf("\"\"%s\"\":%d", key, rand.Int31()))
			case metadata.MetricsTypeInt8:
				vs = append(vs, fmt.Sprintf("\"\"%s\"\":%d", key, rand.Int63()))
			case metadata.MetricsTypeFloat4:
				vs = append(vs, fmt.Sprintf("\"\"%s\"\":%f", key, rand.Float32()))
			case metadata.MetricsTypeFloat8:
				vs = append(vs, fmt.Sprintf("\"\"%s\"\":%f", key, rand.Float64()))
			}
		} else {
			switch columnsDesc.MetricsType {
			case metadata.MetricsTypeInt4:
				vs = append(vs, fmt.Sprintf("\"\"%s\"\":%d", key, rand.Int31n(int32(columnsDesc.Spec.Max-columnsDesc.Spec.Min))+int32(columnsDesc.Spec.Min)))
			case metadata.MetricsTypeInt8:
				vs = append(vs, fmt.Sprintf("\"\"%s\"\":%d", key, rand.Int63n(int64(columnsDesc.Spec.Max-columnsDesc.Spec.Min))+int64(columnsDesc.Spec.Min)))
			case metadata.MetricsTypeFloat4:
				vs = append(vs, fmt.Sprintf("\"\"%s\"\":%f", key, rand.Float32()*float32(columnsDesc.Spec.Max-columnsDesc.Spec.Min)+float32(columnsDesc.Spec.Min)))
			case metadata.MetricsTypeFloat8:
				vs = append(vs, fmt.Sprintf("\"\"%s\"\":%f", key, rand.Float64()*(columnsDesc.Spec.Max-columnsDesc.Spec.Min)+columnsDesc.Spec.Min))
			}
		}

	}
	buff.WriteString(strings.Join(vs, ","))
	buff.WriteString("}\"")
	return buff.String()

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
			keys[i] = fmt.Sprintf("k%d", i)
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
