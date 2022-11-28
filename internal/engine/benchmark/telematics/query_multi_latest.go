package telematics

import (
	"fmt"

	"github.com/ymatrix-data/mxbench/internal/engine"
	"github.com/ymatrix-data/mxbench/internal/engine/metadata"
)

const (
	_LATEST_QUERY_TAG_NUM              = 10
	_QUERY_NAME_MULTI_TAG_LATEST_QUERY = "MULTI_TAG_LATEST_QUERY"

	_MULTI_TAG_LATEST_QUERY = `SELECT
    %[1]s
FROM %[2]s AS %[3]s
INNER JOIN (
	SELECT
	    %[4]s,
	    MAX(%[5]s) AS max_ts
	FROM %[6]s
	WHERE %[7]s IN ( %%s )
	GROUP BY %[7]s
    ) AS %[8]s
ON %[3]s.%[7]s = %[8]s.%[7]s AND %[3]s.%[5]s=%[8]s.max_ts`
	_MULTI_TAG_LATEST_QUERY_WITH_JSON_METRICS = `SELECT
    %[1]s
FROM %[2]s AS %[3]s
INNER JOIN (
	SELECT %[4]s,
	MAX(%[5]s) AS max_ts
	FROM %[6]s
	WHERE %[7]s IN ( %%s )
	GROUP BY %[7]s) AS %[8]s
ON %[3]s.%[7]s = %[8]s.%[7]s AND %[3]s.%[5]s=%[8]s.max_ts
, %[12]s_to_record(%[3]s.%[9]s) AS %[10]s( %[11]s )`
)

type queryMultiLatest struct {
	format             string
	multiVinsGenerator metadata.MultiVinsGenerator
}

func (q *queryMultiLatest) GetSQL() string {
	return fmt.Sprintf(q.format, q.multiVinsGenerator())
}

func (q *queryMultiLatest) GetName() string {
	return _QUERY_NAME_MULTI_TAG_LATEST_QUERY
}

func newQueryMultiLatest(meta *metadata.Metadata, cfg *Config) engine.Query {
	tableIdentifier := meta.Table.Identifier()
	simpleMetricsCount, jsonMetricsCount, _ := getQueryParams(meta, cfg)
	if meta.Table.JSONMetricsCount == 0 {
		return &queryMultiLatest{
			format: fmt.Sprintf(_MULTI_TAG_LATEST_QUERY,
				meta.Table.Columns[:simpleMetricsCount+metadata.NON_METRICS_COLUMN_NUM].ToSelectSQLStr(_RELATION_ALIAS_R1),
				tableIdentifier,
				_RELATION_ALIAS_R1,
				meta.Table.Columns[1:2].ToSelectSQLStr(""),
				meta.Table.ColumnNameTS,
				tableIdentifier,
				meta.Table.ColumnNameVIN,
				_RELATION_ALIAS_R2,
			),
			multiVinsGenerator: meta.GetRandomVinsGenerator(_LATEST_QUERY_TAG_NUM),
		}
	}
	return &queryMultiLatest{
		format: fmt.Sprintf(_MULTI_TAG_LATEST_QUERY_WITH_JSON_METRICS,
			meta.Table.Columns[metadata.NON_METRICS_COLUMN_NUM:simpleMetricsCount+metadata.NON_METRICS_COLUMN_NUM].ToSelectSQLStr(_RELATION_ALIAS_R1)+
				"\n  , "+meta.ToJSONSelectStr(meta.Table.ColumnsDescsExt, _RELATION_ALIAS_R2, jsonMetricsCount),
			tableIdentifier,
			_RELATION_ALIAS_R1,
			meta.Table.Columns[1:2].ToSelectSQLStr(""),
			meta.Table.ColumnNameTS,
			tableIdentifier,
			meta.Table.ColumnNameVIN,
			_RELATION_ALIAS_R3,
			meta.Table.ColumnNameExt,
			_RELATION_ALIAS_R2,
			meta.ToJSONColStr(meta.Table.ColumnsDescsExt, jsonMetricsCount),
			meta.Table.ColumnTypeExt,
		),
		multiVinsGenerator: meta.GetRandomVinsGenerator(_LATEST_QUERY_TAG_NUM),
	}
}
