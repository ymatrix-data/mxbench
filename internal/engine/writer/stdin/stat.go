package stdin

import (
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/jedib0t/go-pretty/v6/list"
	"github.com/jedib0t/go-pretty/v6/table"
	"github.com/jedib0t/go-pretty/v6/text"

	"github.com/ymatrix-data/mxbench/internal/engine"
	"github.com/ymatrix-data/mxbench/internal/util"
	"github.com/ymatrix-data/mxbench/internal/util/log"
)

type Stat struct {
	startAt, stopAt, lastWatchAt    time.Time
	size, lastWatchSize             int64
	sizeToGate, lastWatchSizeToGate int64
	count, lastWatchCount           int64
	volumeDesc                      engine.VolumeDesc
	config                          *Config
}

func (s *Stat) GetTableCompressRatio() (float64, error) {
	if s.volumeDesc.GetTableSizeFunc == nil {
		return 0, nil
	}
	sizeInDB, err := s.volumeDesc.GetTableSizeFunc()
	if err != nil {
		return 0, err
	}
	sizeWritten := s.size
	compressRatio := float64(0)
	if sizeWritten > 0 {
		compressRatio = float64(s.sizeToGate) / float64(sizeInDB)
	}
	return compressRatio, nil
}

// GetSummary is aimed at presenting statistics to the user
// in the form of a table empowered by go-pretty.
// Any data in float will be rounded to 2 decimal places.
func (s *Stat) GetSummary() string {
	if s.volumeDesc.GetTableSizeFunc == nil {
		return ""
	}
	tbl := table.NewWriter()
	tbl.SetStyle(table.StyleLight)

	compressRatio, err := s.GetTableCompressRatio()
	if err != nil {
		return ""
	}
	tbl.AppendRows([]table.Row{
		{"start time:", s.startAt.Format(s.config.getProgressTimeLayout())},
		{"stop time:", s.stopAt.Format(s.config.getProgressTimeLayout())},
		{"size written to mxgate (bytes):", s.sizeToGate},
		{"lines inserted:", s.count},
		{"compress ratio:", fmt.Sprintf("%.4f : 1", compressRatio)},
	})
	// Set Style
	tbl.SetColumnConfigs([]table.ColumnConfig{
		{Name: "Default", Align: text.AlignCenter, AlignHeader: text.AlignCenter},
	})
	tbl.SetStyle(table.StyleLight)
	tbl.Style().Title.Align = text.AlignCenter
	tbl.Style().Options.SeparateRows = true
	tbl.SetTitle("Summary Report for Stdin Writer")

	return tbl.Render()
}

// GetFormattedSummary is aimed at outputing statistics in certain format.
// No data in will be rounded.
func (s *Stat) GetFormattedSummary() string {
	if s.volumeDesc.GetTableSizeFunc == nil {
		return ""
	}
	startTime := s.startAt.Format(s.config.getProgressTimeLayout())
	stopTime := s.stopAt.Format(s.config.getProgressTimeLayout())
	sizeWrittenToMxgateBytes := fmt.Sprintf("%d", s.sizeToGate)
	insertedLines := fmt.Sprintf("%d", s.count)
	compressRatio, _ := s.GetTableCompressRatio()
	writeReports := []string{startTime, stopTime, sizeWrittenToMxgateBytes, insertedLines, fmt.Sprintf("%f", compressRatio)}
	row := strings.Join(writeReports, util.DELIMITER)
	return row
}

func (s *Stat) AddSubStat(engine.Stat) {}
func (s *Stat) GetSubStats() []engine.Stat {
	return nil
}
func (s *Stat) GetName() string {
	return ""
}

func (s *Stat) GetProgress() string {
	if s.startAt.IsZero() {
		return "start time is not set"
	}

	switch s.config.ProgressFormat {
	case "json":
		return s.getProgressWithJSONStr()
	case "list":
		return s.getProgressWithListStr()
	default:
		return "progress info does not support format: " + s.config.ProgressFormat
	}
}

// getProgressWithListStr is aimed at presenting statistics to the user
// in the form of a table empowered by go-pretty.
// Any data in float will be rounded to 2 decimal places.
func (s *Stat) getProgressWithListStr() string {
	start := s.startAt

	l := list.NewWriter()
	l.SetStyle(list.StyleBulletCircle)

	l.AppendItem("Stdin Writer Report")
	l.Indent()

	if !s.stopAt.IsZero() {
		l.AppendItem("100% generating completed")
		fmt.Println(l.Render())
		return ""
	}
	now := time.Now()
	if s.lastWatchAt.IsZero() {
		s.lastWatchAt = start
	}
	start = s.lastWatchAt

	l.AppendItem(fmt.Sprintf("period start: %s, end: %s, period: %.2f seconds\n", start.Format(s.config.getProgressTimeLayout()), now.Format(s.config.getProgressTimeLayout()),
		now.Sub(s.lastWatchAt).Seconds()))

	count := s.volumeDesc.GeneratorPrediction.Count
	estimatedSize := s.volumeDesc.GeneratorPrediction.Size

	countStatItem := fmt.Sprintf("count written in total: %d rows, %d rows in this period\n", s.count,
		s.count-s.lastWatchCount)
	if count > 0 {
		countStatItem = fmt.Sprintf("count written in total: %d rows/ %d rows %.2f%%, %d rows in this period\n", s.count, count,
			100*float64(s.count)/float64(count),
			s.count-s.lastWatchCount)
	}

	l.AppendItem(countStatItem)

	sizeProgressPercentile := 100 * float64(s.size) / float64(estimatedSize)
	if sizeProgressPercentile > 100 {
		sizeProgressPercentile = 100
	}
	l.AppendItem(fmt.Sprintf("size written in total: %d bytes/ %d bytes %.2f%%, %d bytes in this period\n", s.size, estimatedSize,
		sizeProgressPercentile,
		s.size-s.lastWatchSize))
	l.AppendItem(fmt.Sprintf("size written to mxgate in total: %d bytes, %d bytes in this period\n", s.sizeToGate,
		s.sizeToGate-s.lastWatchSizeToGate))
	if s.config.ProgressIncludeTableSize && s.volumeDesc.GetTableSizeFunc != nil {
		tableSize, _ := s.volumeDesc.GetTableSizeFunc()
		l.AppendItem(fmt.Sprintf("table size: %d bytes\n", tableSize))
	}

	s.lastWatchAt = now
	s.lastWatchCount = s.count
	s.lastWatchSize = s.size
	s.lastWatchSizeToGate = s.sizeToGate

	return l.Render()
}

// getProgressWithJSONStr is aimed at outputing statistics in JSON.
// No data in will be rounded.
func (s *Stat) getProgressWithJSONStr() string {
	progress := &WriterProgress{}
	now := time.Now()
	if s.lastWatchAt.IsZero() {
		s.lastWatchAt = s.startAt
	}
	start := s.lastWatchAt
	progress.Start = start.Format(s.config.getProgressTimeLayout())
	progress.End = now.Format(s.config.getProgressTimeLayout())
	progress.Period = now.Sub(s.lastWatchAt).String()

	progress.CurrTotalRows = s.count
	progress.CurrPeriodRows = s.count - s.lastWatchCount
	if s.volumeDesc.GeneratorPrediction.Count > 0 {
		progress.TotalRows = s.volumeDesc.GeneratorPrediction.Count
	}

	progress.CurrTotalBytes = s.size
	progress.CurrPeriodBytes = s.size - s.lastWatchSize
	progress.TotalBytes = s.volumeDesc.GeneratorPrediction.Size

	progress.WrittenMxgateTotal = s.sizeToGate
	progress.CurrPeriodWrittenMxgate = s.sizeToGate - s.lastWatchSizeToGate
	if s.config.ProgressIncludeTableSize && s.volumeDesc.GetTableSizeFunc != nil {
		progress.TableSize, _ = s.volumeDesc.GetTableSizeFunc()
	}

	s.lastWatchAt = now
	s.lastWatchCount = s.count
	s.lastWatchSize = s.size
	s.lastWatchSizeToGate = s.sizeToGate

	b, err := json.Marshal(&progress)
	if err != nil {
		log.Warn("failed to marshal progress to json: %v", err)
	}

	return string(b)
}

func (s *Stat) GetCurrentProgress(_ ...interface{}) map[string]interface{} {
	// placeholder
	return nil
}
