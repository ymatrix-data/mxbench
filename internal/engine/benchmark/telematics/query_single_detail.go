package telematics

import (
	"fmt"
	"time"

	"github.com/ymatrix-data/mxbench/internal/engine"
	"github.com/ymatrix-data/mxbench/internal/engine/metadata"
)

const (
	_DETAIL_QUERY_DURATION              = time.Hour
	_QUERY_NAME_SINGLE_TAG_DETAIL_QUERY = "SINGLE_TAG_DETAIL_QUERY"

	_SINGLE_TAG_DETAIL_QUERY = `SELECT ARRAY[
%[1]s ]
FROM %[2]s
WHERE %[3]s = %%s
AND %[4]s >= %%s
AND %[4]s < %%s`
	_SINGLE_TAG_DETAIL_QUERY_WITH_JSON_METRICS = `SELECT
ARRAY[%[1]s]
FROM %[2]s AS %[3]s
, %[9]s_to_record(%[3]s.%[4]s) AS %[5]s ( %[6]s )
WHERE %[7]s = %%s
AND %[8]s >= %%s
AND %[8]s < %%s`
	_SINGLE_TAG_DETAIL_QUERY_WITH_JSON_METRICS_MXKV2 = `SELECT
ARRAY[%[1]s]
FROM %[2]s AS %[3]s
WHERE %[7]s = %%s
AND %[8]s >= %%s
AND %[8]s < %%s`
)

type querySingleDetail struct {
	format             string
	singleVinGenerator metadata.SingleVinGenerator
	durationGenerator  metadata.DurationGenerator
}

func (q *querySingleDetail) GetSQL() string {
	start, end := q.durationGenerator()
	return fmt.Sprintf(q.format, q.singleVinGenerator(), start, end)
}

func (q *querySingleDetail) GetName() string {
	return _QUERY_NAME_SINGLE_TAG_DETAIL_QUERY
}

func newQuerySingleDetail(meta *metadata.Metadata, cfg *Config) engine.Query {
	tableIdentifier := meta.Table.Identifier()
	simpleMetricsCount, jsonMetricsCount, durationGenerator := getQueryParams(meta, cfg)
	if meta.Table.JSONMetricsCount == 0 {
		return &querySingleDetail{
			format: fmt.Sprintf(_SINGLE_TAG_DETAIL_QUERY,
				meta.Table.Columns[metadata.NON_METRICS_COLUMN_NUM:simpleMetricsCount+metadata.NON_METRICS_COLUMN_NUM].ToSelectSQLStr(""),
				tableIdentifier,
				meta.Table.ColumnNameVIN,
				meta.Table.ColumnNameTS,
			),
			singleVinGenerator: meta.GetSingleVinGenerator(),
			durationGenerator:  durationGenerator,
		}
	}
	return &querySingleDetail{
		format: fmt.Sprintf(_SINGLE_TAG_DETAIL_QUERY_WITH_JSON_METRICS_MXKV2,
			meta.Table.Columns[metadata.NON_METRICS_COLUMN_NUM:simpleMetricsCount+metadata.NON_METRICS_COLUMN_NUM].ToSelectSQLStr(_RELATION_ALIAS_R1)+
				"\n  , "+meta.ToJSONArrowColStr(meta.Table.ColumnsDescsExt, _RELATION_ALIAS_R1, jsonMetricsCount),
			tableIdentifier,
			_RELATION_ALIAS_R1,
			meta.Table.ColumnNameExt,
			_RELATION_ALIAS_R2,
			meta.ToJSONColStr(meta.Table.ColumnsDescsExt, jsonMetricsCount),
			meta.Table.ColumnNameVIN,
			meta.Table.ColumnNameTS,
			meta.Table.ColumnTypeExt,
		),
		singleVinGenerator: meta.GetSingleVinGenerator(),
		durationGenerator:  durationGenerator,
	}
}
