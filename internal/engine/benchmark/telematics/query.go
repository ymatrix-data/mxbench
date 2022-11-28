package telematics

import (
	"fmt"
	"strings"

	"github.com/ymatrix-data/mxbench/internal/engine"
	"github.com/ymatrix-data/mxbench/internal/engine/metadata"
)

const (
	_RELATION_ALIAS_R1 = "t1"
	_RELATION_ALIAS_R2 = "t2"
	_RELATION_ALIAS_R3 = "t3"
)

var allQueryNames = []string{
	_QUERY_NAME_SINGLE_TAG_LATEST_QUERY,
	_QUERY_NAME_MULTI_TAG_LATEST_QUERY,
	_QUERY_NAME_SINGLE_TAG_DETAIL_QUERY,
}

var queryNameNewFunc = map[string]func(*metadata.Metadata, *Config) engine.Query{
	_QUERY_NAME_SINGLE_TAG_LATEST_QUERY: newQuerySingleLatest,
	_QUERY_NAME_MULTI_TAG_LATEST_QUERY:  newQueryMultiLatest,
	_QUERY_NAME_SINGLE_TAG_DETAIL_QUERY: newQuerySingleDetail,
}

func getQueryNamesInConfigFile() string {
	queryNames := make([]string, 0, len(queryNameNewFunc))
	for _, queryName := range allQueryNames {
		queryNames = append(queryNames, fmt.Sprintf("\"%s\"", queryName))
	}
	return fmt.Sprintf("[ %s ]", strings.Join(queryNames, ", "))
}

func getQueryParams(meta *metadata.Metadata, cfg *Config) (int64, int64, metadata.DurationGenerator) {
	jsonMetricsCount := meta.Table.JSONMetricsCount
	simpleMetricsCount := meta.Table.TotalMetricsCount - meta.Table.JSONMetricsCount
	durationGenerator := meta.GetRandomStartEndTSArgGenerator(_DETAIL_QUERY_DURATION)
	if cfg != nil {
		jsonMetricsCount = cfg.JSONMetricsCount
		if cfg.SimpleMetricsCount < simpleMetricsCount {
			simpleMetricsCount = cfg.SimpleMetricsCount
		}
		if cfg.TimestampStart != "" && cfg.TimestampEnd != "" {
			durationGenerator = meta.GetFixedStartEndTSArgGenerator(cfg.TimestampStart, cfg.TimestampEnd)
		}
	}
	return simpleMetricsCount, jsonMetricsCount, durationGenerator
}
