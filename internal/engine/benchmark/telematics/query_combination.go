package telematics

import (
	"encoding/json"
	"strconv"
	"strings"
	"time"

	"github.com/ymatrix-data/mxbench/internal/engine"
	"github.com/ymatrix-data/mxbench/internal/engine/metadata"
	"github.com/ymatrix-data/mxbench/internal/util"
)

const _AVG_COMBINATION_QUERIES_NUM = 20

type queryCombination struct {
	Name               string              `json:"name"`
	Projections        *projections        `json:"projections"`
	FromExpression     *fromExpression     `json:"from"`
	DevicePredicate    *devicePredicate    `json:"device-predicate"`
	TimestampPredicate *timestampPredicate `json:"ts-predicate"`
	MetricsPredicate   *metricsPredicate   `json:"metrics-predicate"`
	GroupByPredicate   *groupByPredicate   `json:"group-by"`
	OrderByPredicate   *orderByPredicate   `json:"order-by"`
	Limit              int                 `json:"limit"`

	meta *metadata.Metadata
	cfg  *Config
}

func (q *queryCombination) GetSQL() string {
	// Projections
	if q.Projections == nil {
		return ""
	}
	sql := "SELECT " + q.Projections.GetStr()

	// From Expression
	if q.FromExpression == nil {
		sql += "\nFROM " + q.meta.Table.Identifier()
	} else {
		sql += "\nFROM " + q.FromExpression.GetStr()
	}

	// Predicates
	firstPredicate := true
	if q.DevicePredicate != nil {
		if firstPredicate {
			firstPredicate = false
			sql += "\nWHERE " + q.DevicePredicate.GetStr(q.meta, q.cfg)
		} else {
			sql += "\nAND " + q.DevicePredicate.GetStr(q.meta, q.cfg)
		}
	}
	if q.TimestampPredicate != nil {
		if firstPredicate {
			firstPredicate = false
			sql += "\nWHERE " + q.TimestampPredicate.GetStr(q.meta, q.cfg)
		} else {
			sql += "\nAND " + q.TimestampPredicate.GetStr(q.meta, q.cfg)
		}
	}
	if q.MetricsPredicate != nil {
		if firstPredicate {
			sql += "\nWHERE " + q.MetricsPredicate.GetStr()
		} else {
			sql += "\nAND " + q.MetricsPredicate.GetStr()
		}
	}

	// Group By Expression
	if q.GroupByPredicate != nil {
		sql += "\nGROUP BY " + q.GroupByPredicate.GetStr()
	}

	// Order By Expression
	if q.OrderByPredicate != nil {
		sql += "\nORDER BY " + q.OrderByPredicate.GetStr()
	}

	// Limit Expression
	if q.Limit > 0 {
		sql += "\nLIMIT " + strconv.Itoa(q.Limit)
	}

	return sql + "\n"
}

func (q *queryCombination) GetName() string {
	return q.Name
}

// parseCombinationQueries takes a string formatted in a JSON array,
// and transform it to an array of engine.Query, with the possibility of error occurring
// when combinationQueries is ill-formatted
// A example of a "good" "combinationQueries" might be:·
// '[
//     {
//      "projections": {"use-raw-expression": true, "expression": "*"},
//      "device-predicate": {"count": 2, "is-random": true},
//      "ts-predicate": {"start": "2022-05-05 01:04:10", "end": "2022-05-05 01:04:10"}
//     },
// 	   {
// 	 	"projections": {"use-raw-expression": true, "expression": "*"},
// 	 	"from":  {
// 	 	"use-relation-name": false,
// 	   	"non-statement": false,
// 	 	"relation-statement":
// 	 		{
// 	 		"projections": {"use-raw-expression": true, "expression": "autoid,max(actpress) as mp, min(cycletime), count(actpress), avg(weldcol)"},
// 	 		"ts-predicate": {"start": "2022-05-03 00:00:00", "end": "2022-05-04 00:00:00"},
// 	 		"group-by": {"use-raw-expression": true, "expression": "autoid"}
// 	 		}
// 	 	},
// 	 	"order-by": {"use-raw-expression": true, "expression": "mp desc"},
// 	 	"limit": 10
// 	 	}
//  ]'
func parseCombinationQueries(combinationQueries string) ([]*queryCombination, error) {
	// if "combinationQueries" is an empty string, return a nil slice and no error
	combinationQueries = strings.TrimSpace(combinationQueries)
	if combinationQueries == "" {
		return nil, nil
	}

	queries := make([]*queryCombination, 0, _AVG_COMBINATION_QUERIES_NUM)
	err := json.Unmarshal([]byte(combinationQueries), &queries)
	if err != nil {
		return nil, err
	}

	// deal with timestamp
	for i := range queries {
		if queries[i].TimestampPredicate == nil {
			continue
		}
		start, end := queries[i].TimestampPredicate.Start, queries[i].TimestampPredicate.End
		if start != "" {
			queries[i].TimestampPredicate.StartTime, err = time.Parse(util.TIME_FMT, start)
			if err != nil {
				return nil, err
			}
		}
		if end != "" {
			queries[i].TimestampPredicate.EndTime, err = time.Parse(util.TIME_FMT, end)
			if err != nil {
				return nil, err
			}
		}
	}

	return queries, nil
}

func transformCombinationQueries(queries []*queryCombination, meta *metadata.Metadata, cfg *Config) []engine.Query {
	results := make([]engine.Query, 0, len(queries))
	for i := range queries {
		assignMetaCfgForQuery(queries[i], meta, cfg)
		results = append(results, queries[i])
	}
	return results
}

func assignMetaCfgForQuery(query *queryCombination, meta *metadata.Metadata, cfg *Config) {
	query.meta = meta
	query.cfg = cfg
	if query.FromExpression == nil || query.FromExpression.RelationStatement == nil {
		return
	}
	assignMetaCfgForQuery(query.FromExpression.RelationStatement, meta, cfg)
}
