package telematics

import (
	"encoding/json"
	"fmt"
	"strconv"
	"strings"

	"github.com/jedib0t/go-pretty/v6/list"
	"github.com/jedib0t/go-pretty/v6/table"
	"github.com/jedib0t/go-pretty/v6/text"

	"github.com/ymatrix-data/mxbench/internal/engine"
	"github.com/ymatrix-data/mxbench/internal/util/log"
)

type Stat struct {
	subStats []engine.Stat
	config   *Config
}

func newStat(bCfg *Config) *Stat {
	return &Stat{
		subStats: make([]engine.Stat, 0),
		config:   bCfg,
	}
}

// (s *Stat).arrangeSubStats rearranges s.subStats of []engine.Stat,
// whose structure is like:
// *s_qx_py: engine.Stat, statistics for query "x", with a parallel of "y"
// [
//
//	s_q1_p1, s_q2_p1, s_q3_p1,  statistics of parallel 1 for each query
//	s_q2_p2, s_q2_p2, s_q2_p2,  statistics of parallel 2 for each query
//	...
//
// ]
// and returns it as a slice of [][]engine.Stat, whose structure is like:
// [
//
//	[s_q1_p1, s_q1_p2, s_q1_p3],  statistics of query 1 for each parallel
//	[s_q2_p1, s_q2_p2, s_q2_p3],  statistics of query 2 for each parallel
//	...
//
// ]
func (s *Stat) arrangeSubStats() [][]engine.Stat {
	// subStats: []engine.Query
	// [
	//   s_q1_p1, s_q2_p1, s_q3_p1,  subStats parallel 1 for each query
	//   s_q2_p2, s_q2_p2, s_q2_p2,  subStats parallel 2 for each query
	//   ...
	// ]
	presetQueryNum := len(s.config.RunQueryNames)
	combinationQueryNum := s.config.NumOfParsedCombinationQueries
	cusQueryNum := len(s.config.CustomQueries)
	totalQueryNum := presetQueryNum + combinationQueryNum + cusQueryNum
	parallelNum := len(s.config.Parallel)
	subStatsNum := len(s.subStats)

	// subStatsRearranged [][]engine.Query
	// [
	//   [s_q1_p1, s_q1_p2, s_q1_p3],  subStats query 1 for each parallel
	//   [s_q2_p1, s_q2_p2, s_q2_p3],  subStats query 2 for each parallel
	//   ...
	// ]
	subStatsRearranged := make([][]engine.Stat, 0, totalQueryNum)

	for qIndex := 0; qIndex < totalQueryNum; qIndex++ {
		qIndexSubStats := make([]engine.Stat, 0, parallelNum)
		for pIndex := 0; pIndex < parallelNum; pIndex++ {
			// pick up the subStats for parallel of index pIndex, query of index qIndex
			statIndex := pIndex*totalQueryNum + qIndex
			if statIndex >= subStatsNum {
				// means not executed
				break
			}
			qIndexSubStats = append(qIndexSubStats, s.subStats[statIndex])
		}
		subStatsRearranged = append(subStatsRearranged, qIndexSubStats)
	}
	return subStatsRearranged
}

func (s *Stat) GetSummary() string {
	if len(s.subStats) == 0 {
		return ""
	}
	// subStatsRearranged [][]engine.Query
	// [
	//   [s_q1_p1, s_q1_p2, s_q1_p3],  subStats query 1 for each parallel
	//   [s_q2_p1, s_q2_p2, s_q2_p3],  subStats query 2 for each parallel
	//   ...
	// ]
	subStatsRearranged := s.arrangeSubStats()

	writer := table.NewWriter()

	// Set Header: QUERY NAME\PARALLEL and each parallel
	// ├─────────────────────┬─────────────────────────┬────────────────────────┬
	// │ Query Name\Parallel │ 1                       │ 4                      │
	// ├─────────────────────┼─────────────────────────┼────────────────────────┼
	tableRow := make(table.Row, 0, len(s.config.Parallel)+1)
	tableRow = append(tableRow, "Query Name\\Parallel")
	for _, parallel := range s.config.Parallel {
		tableRow = append(tableRow, parallel)
	}
	writer.AppendRow(tableRow)

	// build rows: each row is a series of subStats a particular query for each parallel
	// [s_q1_p1, s_q1_p2, s_q1_p3],
	for _, subStatsForAQuery := range subStatsRearranged {
		if len(subStatsForAQuery) == 0 {
			break
		}
		tableRow = make(table.Row, 0, len(subStatsForAQuery)+1)
		// first append the name of this query
		// FIXME: might be a little bit hack for getting the name of the queries, but simple
		tableRow = append(tableRow, strings.Split(subStatsForAQuery[0].GetName(), ",")[0])
		for _, subStat := range subStatsForAQuery {
			tableRow = append(tableRow, subStat.GetSummary())
		}
		writer.AppendRow(tableRow)
	}

	// Set Style
	writer.SetColumnConfigs([]table.ColumnConfig{
		{Name: "Default", Align: text.AlignCenter, AlignHeader: text.AlignCenter},
	})
	writer.SetStyle(table.StyleLight)
	writer.Style().Title.Align = text.AlignCenter
	writer.Style().Options.SeparateRows = true
	writer.SetTitle("Summary Report for Telematics Benchmark")
	return writer.Render()
}

type QueryInfo struct {
	QueryName   string `json:"query_name"`
	CustomQuery string `json:"custom_query"`
	Stats       string `json:"stats"`
}

func (s *Stat) GetFormattedSummary() string {
	presetQueryNum := len(s.config.RunQueryNames)
	cusQueryNum := len(s.config.CustomQueries)
	dataWidth := presetQueryNum + cusQueryNum
	var concurrency int
	queryResult := map[string]QueryInfo{}

	for rowNum, parallel := range s.config.Parallel {
		concurrency = parallel
		startIndex := rowNum * dataWidth
		for i := 0; i < presetQueryNum; i++ {
			siIndex := startIndex + i
			if siIndex >= len(s.subStats) {
				break
			}
			queryInfo := QueryInfo{
				QueryName:   s.config.RunQueryNames[i],
				CustomQuery: "",
				Stats:       s.subStats[siIndex].GetFormattedSummary(),
			}
			queryResult[s.config.RunQueryNames[i]] = queryInfo
		}
		// custom query stats
		for i := 0; i < cusQueryNum; i++ {
			siIndex := startIndex + presetQueryNum + i
			if siIndex >= len(s.subStats) {
				break
			}
			queryInfo := QueryInfo{
				QueryName:   _CUSTOM_QUERY_NAME_PREFIX + strconv.Itoa(i+1),
				CustomQuery: s.config.CustomQueries[i],
				Stats:       s.subStats[siIndex].GetFormattedSummary(),
			}
			queryResult[s.config.CustomQueries[i]] = queryInfo
		}
	}
	resStr, err := json.Marshal(queryResult)
	if err != nil {
		log.Error("Failed to tranfer object to json string: [%v]", err)
		return ""
	}
	rows := strconv.Itoa(concurrency) + "|" + string(resStr) + "\n"
	return rows

}

func (s *Stat) GetProgress() string {
	if len(s.subStats) == 0 {
		return ""
	}

	switch s.config.ProgressFormat {
	case "json":
		return s.GetProgressWithJSONStr()
	case "list":
		return s.GetProgressWithListStr()
	default:
		return "benchmark progress info does not support format: " + s.config.ProgressFormat
	}
}

func (s *Stat) GetProgressWithListStr() string {
	l := list.NewWriter()
	l.AppendItem("Telematics Benchmark Report")
	l.Indent()
	for _, ss := range s.subStats {
		l.AppendItem(fmt.Sprintf("%s: progress: %s%%\n", ss.GetName(), ss.GetProgress()))
	}
	l.SetStyle(list.StyleBulletCircle)
	return l.Render()
}

func (s *Stat) GetProgressWithJSONStr() string {
	var output string
	for _, ss := range s.subStats {
		p := ss.GetCurrentProgress()

		bytes, err := json.Marshal(p)
		if bytes != nil {
			output += string(bytes)
		}
		// ! should NEVER fall into err != nil.
		if err != nil {
			output += err.Error()
		}
		output += "\n"
	}
	return output
}

func (s *Stat) AddSubStat(ss engine.Stat) {
	s.subStats = append(s.subStats, ss)
}

func (s *Stat) Reset() {
	s.subStats = make([]engine.Stat, 0)
}

func (s *Stat) GetSubStats() []engine.Stat {
	return s.subStats
}
func (s *Stat) GetName() string {
	return ""
}

func (s *Stat) GetCurrentProgress(_ ...interface{}) map[string]interface{} {
	// placeholder
	return nil
}
