package telematics

import (
	"fmt"

	"github.com/ymatrix-data/mxbench/internal/engine"
	"github.com/ymatrix-data/mxbench/internal/engine/metadata"
)

const (
	_QUERY_NAME_SINGLE_TAG_LATEST_QUERY = "SINGLE_TAG_LATEST_QUERY"

	_SINGLE_TAG_LATEST_QUERY = `SELECT
    %[1]s
FROM %[2]s
WHERE %[3]s = %%s
ORDER BY %[4]s DESC LIMIT 1`
	_SINGLE_TAG_LATEST_QUERY_WITH_JSON_METRICS = `SELECT
    %[1]s
FROM %[2]s AS %[3]s
, %[9]s_to_record(%[3]s.%[4]s) AS %[5]s ( %[6]s )
WHERE %[7]s = %%s
ORDER BY %[8]s DESC LIMIT 1`
	_SINGLE_TAG_LATEST_QUERY_WITH_JSON_METRICS_MXKV2 = `SELECT
    %[1]s
FROM %[2]s AS %[3]s
WHERE %[7]s = %%s
ORDER BY %[8]s DESC LIMIT 1`
)

type querySingleLatest struct {
	format             string
	singleVinGenerator metadata.SingleVinGenerator
}

func (q *querySingleLatest) GetSQL() string {
	return fmt.Sprintf(q.format, q.singleVinGenerator())
}

func (q *querySingleLatest) GetName() string {
	return _QUERY_NAME_SINGLE_TAG_LATEST_QUERY
}

func newQuerySingleLatest(meta *metadata.Metadata, cfg *Config) engine.Query {
	tableIdentifier := meta.Table.Identifier()
	simpleMetricsCount, jsonMetricsCount, _ := getQueryParams(meta, cfg)
	if meta.Table.JSONMetricsCount == 0 {
		return &querySingleLatest{
			format: fmt.Sprintf(
				_SINGLE_TAG_LATEST_QUERY,
				meta.Table.Columns[:simpleMetricsCount+metadata.NON_METRICS_COLUMN_NUM].ToSelectSQLStr(""),
				tableIdentifier,
				meta.Table.ColumnNameVIN,
				meta.Table.ColumnNameTS,
			),
			singleVinGenerator: meta.GetSingleVinGenerator(),
		}
	}
	return &querySingleLatest{
		format: fmt.Sprintf(_SINGLE_TAG_LATEST_QUERY_WITH_JSON_METRICS_MXKV2,
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
	}
}
