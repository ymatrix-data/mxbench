package engine

import (
	"encoding/json"
	"fmt"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jedib0t/go-pretty/v6/table"

	"github.com/ymatrix-data/mxbench/internal/util/log"
)

type ReportFormat = string

const (
	ReportFormatCSV  ReportFormat = "csv"
	ReportFormatJSON ReportFormat = "json"
)

type Stat interface {
	AddSubStat(Stat)

	GetName() string
	GetSummary() string
	GetFormattedSummary(prefix string) string
	GetProgress() string
	GetSubStats() []Stat

	GetCurrentProgress(opt ...interface{}) map[string]interface{}
}

func NewExecBenchStat(opt ExecBenchOption, query Query) *ExecBenchStat {
	return &ExecBenchStat{
		latencies:    make([]time.Duration, 0, 100*opt.Parallel), // 100 is the default of run times
		opt:          opt,
		query:        query,
		reportFormat: ReportFormatJSON,
	}
}

type ExecBenchStat struct {
	TPS        int           `json:"tps"`
	AvgLatency time.Duration `json:"avg-latency"`
	MaxLatency time.Duration `json:"max-latency"`
	P75Latency time.Duration `json:"p75-latency"`
	P50Latency time.Duration `json:"p50-latency"`
	P25Latency time.Duration `json:"p25-latency"`

	mu        sync.Mutex
	latencies []time.Duration

	runs        int64
	TimeElapsed int64 `json:"overall-duration"`

	query Query
	opt   ExecBenchOption

	reportFormat ReportFormat
}

func (ebs *ExecBenchStat) GetName() string {
	return fmt.Sprintf("stats for query %s, with parallel %d", ebs.query.GetName(), ebs.opt.Parallel)
}

// GetSummary is aimed at presenting statistics to the user
// in the form of a table empowered by go-pretty.
// Any data in float will be rounded to 2 decimal places.
func (ebs *ExecBenchStat) GetSummary() string {
	ebs.complete()
	if len(ebs.latencies) == 0 {
		return "not actually executed"
	}
	tbl := table.NewWriter()
	tbl.ResetRows()
	tbl.SetStyle(table.StyleLight)

	tbl.SetCaption("progress: %s%%", ebs.GetProgress())

	tbl.AppendRows([]table.Row{
		{"Overall Duration", fmt.Sprintf("%.2fs", float64(time.Duration(atomic.LoadInt64(&ebs.TimeElapsed)))/1e9)},
		{"Average Latency", fmt.Sprintf("%.3fms", float64(ebs.AvgLatency.Nanoseconds())/1e6)},
		{"P75 Latency", fmt.Sprintf("%.3fms", float64(ebs.P75Latency.Nanoseconds())/1e6)},
		{"P50 Latency", fmt.Sprintf("%.3fms", float64(ebs.P50Latency.Nanoseconds())/1e6)},
		{"P25 Latency", fmt.Sprintf("%.3fms", float64(ebs.P25Latency.Nanoseconds())/1e6)},
		{"TPS", ebs.TPS},
	})

	return tbl.Render()
}

// GetFormattedSummary is aimed at outputing statistics in certain format.
// No data in will be rounded.
func (ebs *ExecBenchStat) GetFormattedSummary(string) string {
	ebs.complete()
	if len(ebs.latencies) == 0 {
		return "not actually executed"
	}

	switch ebs.reportFormat {
	case ReportFormatJSON:
		b, err := json.Marshal(ebs)
		if err != nil {
			log.Error("error occurs marshaling ebs: %v", err)
			return ""
		}
		return string(b)
	default:
		return ""
	}
}

func (ebs *ExecBenchStat) AddSubStat(_ Stat) {}

func (ebs *ExecBenchStat) GetSubStats() []Stat {
	return nil
}

func (ebs *ExecBenchStat) GetProgress() string {
	if ebs.opt.RunTimes <= 0 {
		// a little bit hack
		progress := int(100*float64(ebs.TimeElapsed)/float64(ebs.opt.Duration)) + 1
		if progress > 100 {
			progress = 100
		}
		return fmt.Sprintf("%d", progress)

	}
	return fmt.Sprintf("%d", int(100*float64(ebs.runs)/float64(ebs.opt.RunTimes*int64(ebs.opt.Parallel))))
}

func (ebs *ExecBenchStat) addLatency(latency time.Duration) {
	ebs.mu.Lock()
	defer ebs.mu.Unlock()
	if ebs.latencies == nil {
		ebs.latencies = []time.Duration{latency}
		return
	}
	ebs.latencies = append(ebs.latencies, latency)
}

func (ebs *ExecBenchStat) complete() {
	latencies := ebs.latencies
	numOfLatencies := len(latencies)
	if numOfLatencies == 0 {
		return
	}
	sort.Slice(latencies, func(i, j int) bool {
		return latencies[i] < latencies[j]
	})
	var latencySum time.Duration
	for _, l := range latencies {
		latencySum += l
	}
	ebs.AvgLatency = latencySum / time.Duration(numOfLatencies)
	ebs.MaxLatency = latencies[numOfLatencies-1]
	ebs.P75Latency = latencies[numOfLatencies*3/4]
	ebs.P50Latency = latencies[numOfLatencies/2]
	ebs.P25Latency = latencies[numOfLatencies/4]
	ebs.TPS = int(float64(numOfLatencies) / float64(time.Duration(ebs.TimeElapsed).Seconds()))
}

func (ebs *ExecBenchStat) GetCurrentProgress(_ ...interface{}) map[string]interface{} {
	ebs.mu.Lock()
	defer ebs.mu.Unlock()

	p := make(map[string]interface{})

	// 1. prepare latency data
	latencies := ebs.latencies
	numOfLatencies := len(latencies)
	if numOfLatencies == 0 {
		return nil
	}
	sort.Slice(latencies, func(i, j int) bool {
		return latencies[i] < latencies[j]
	})
	var latencySum time.Duration
	for _, l := range latencies {
		latencySum += l
	}

	// 2. calc finishedPercentage
	percentage := 0
	if ebs.opt.RunTimes <= 0 {
		// a little bit hack
		percentage = int(100*float64(ebs.TimeElapsed)/float64(ebs.opt.Duration)) + 1
		if percentage > 100 {
			percentage = 100
		}
	} else {
		percentage = int(100 * float64(ebs.runs) / float64(ebs.opt.RunTimes*int64(ebs.opt.Parallel)))
	}

	// 3. build full status
	p["name"] = ebs.GetName()
	p["module"] = "benchmark"
	p["timeElapsed"] = time.Duration(atomic.LoadInt64(&ebs.TimeElapsed))
	p["avgLatency"] = latencySum / time.Duration(numOfLatencies)
	p["maxLatency"] = latencies[numOfLatencies-1]
	p["p75Latency"] = latencies[numOfLatencies*3/4]
	p["p50Latency"] = latencies[numOfLatencies/2]
	p["p25Latency"] = latencies[numOfLatencies/4]
	p["tps"] = int(float64(numOfLatencies) / float64(time.Duration(ebs.TimeElapsed).Seconds()))
	p["finishedPercentage"] = percentage

	return p
}
