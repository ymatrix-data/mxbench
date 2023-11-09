package telematics

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"math/rand"
	"strings"
	"sync"
	"time"

	"github.com/spf13/pflag"
	"golang.org/x/sync/errgroup"

	"github.com/ymatrix-data/mxbench/internal/engine"
	"github.com/ymatrix-data/mxbench/internal/engine/generator/telematics/typ"
	"github.com/ymatrix-data/mxbench/internal/engine/metadata"
	"github.com/ymatrix-data/mxbench/internal/util"
	"github.com/ymatrix-data/mxbench/internal/util/log"
	"github.com/ymatrix-data/mxbench/internal/util/mxerror"
	"github.com/ymatrix-data/mxbench/pkg/mxmock"
)

type Generator struct {
	ctx        context.Context
	cancelFunc context.CancelFunc
	cfg        *Config
	gcfg       engine.GlobalConfig
	meta       *metadata.Metadata
	writeFunc  engine.WriteFunc
	wg         sync.WaitGroup

	cacheBuff []*bytes.Buffer
}

func NewGenerator(cfg engine.GeneratorConfig) engine.IGenerator {
	gCfg := cfg.PluginConfig.(*Config)
	gCfg.init()

	cacheBuff := make([]*bytes.Buffer, gCfg.NumGoRoutine)
	for i := 0; i < gCfg.NumGoRoutine; i++ {
		cacheBuff[i] = bytes.NewBuffer(make([]byte, 0, gCfg.WriteBatchSize*_MEGA_BYTES/gCfg.NumGoRoutine))
	}

	ctx, cancel := context.WithCancel(context.Background())
	return &Generator{
		gcfg:       *cfg.GlobalConfig,
		cfg:        gCfg,
		ctx:        ctx,
		cancelFunc: cancel,
		cacheBuff:  cacheBuff,
	}
}

func (g *Generator) GetPrediction(table *metadata.Table) (engine.GeneratorPrediction, error) {
	linesPerRow := float64(g.cfg.BatchSize)
	linesInTable := float64(g.gcfg.TagNum) * float64(int64(g.gcfg.EndAt.Sub(g.gcfg.StartAt))/
		(int64(g.gcfg.TimestampStepInSecond)*int64(time.Second)))

	amountSize := float64(metadata.ColumnSizeTimestamp+metadata.ColumnSizeVin) + float64(table.SingleRowMetricsSize())
	amountSize = amountSize * linesInTable *
		(1 - float64(g.cfg.emptyValueRatio)/100)

	return engine.GeneratorPrediction{
		Count: int64(linesPerRow * linesInTable),
		Size:  int64(amountSize),
	}, nil
}

func (g *Generator) ModifyMetadataConfig(metaConfig *metadata.Config) {
	if metaConfig == nil {
		return
	}
	metaConfig.HasUniqueConstraints = g.cfg.BatchSize > 1
	metaConfig.EmptyValueRatio = g.cfg.EmptyValueRatio
}

func (g *Generator) Run(cfg engine.GlobalConfig, meta *metadata.Metadata, writeFunc engine.WriteFunc) error {
	g.gcfg = cfg
	g.meta = meta
	g.writeFunc = writeFunc

	err := g.validate()
	if err != nil {
		return err
	}

	g.wg.Add(1)
	defer g.wg.Done()

	log.Info("[Generator.TELEMATICS] Start to load to writer")

	typ.Init(meta.Table)

	tpl, err := g.genTpl()
	if err != nil {
		return err
	}
	if len(tpl) == 0 || len(tpl[0]) == 0 {
		log.Info("[Generator.TELEMATICS] No data generated")
		return nil
	}

	// can get columns here
	// g.meta.Table.Columns
	for _, column := range g.meta.Table.Columns {
		log.Info("[Generator.TELEMATICS] column: %s, type: %s", column.Name, column.TypeName)
	}

	if g.meta.Table.ExtColumn != nil {
		log.Info("[Generator.TELEMATICS] ext column: %s, type: %s", g.meta.Table.ExtColumn.Name, g.meta.Table.ExtColumn.TypeName)
	}
	/*
		for _, column := range g.meta.Table.ExtColumn {
			log.Info("[Generator.TELEMATICS] ext column: %s", column.Name)
		}
	*/

	err = g.write(tpl)
	if err != nil {
		return err
	}

	for _, column := range g.meta.Table.Columns {
		if column.TypeName != metadata.MetricsTypeJSON && column.TypeName != metadata.MetricsTypeJSONB {
			continue
		}

		valueRange := column.GetValueRange()
		if valueRange != nil {
			//column.Comment = valueRange.ToJSON()
			log.Info("[Generator.TELEMATICS] column: %s, type: %s, comment: %s, value range: %+v", column.Name, column.TypeName, column.Comment, valueRange)
		}

		conn, err := util.CreateDBConnection(g.meta.Cfg.DB)
		if err != nil {
			return err
		}

		// add comment on column
		// COMMENT ON COLUMN table1.exttt is '{"is-ext": true, "columns-descriptions": [{"type": "float8", "count": 1, "comment": {"min": 3, "max": 4}},{"type": "float4", "count": 3, "comment": {"min": 2, "max": 3}}]}';

		comment := make(map[string]interface{})
		//comment["columns-descriptions"]
		comments := make([]map[string]interface{}, 0, 10)
		for tp, vr := range valueRange {
			tmp := make(map[string]interface{})
			tmp["type"] = tp
			//tmp["comment"] = make(map[string]int)
			tmp["comment"] = map[string]interface{}{
				"min": vr.Min,
				"max": vr.Max,
			}

			comments = append(comments, tmp)
		}
		comment["columns-descriptions"] = comments

		cm, err := json.Marshal(comment)
		if err != nil {
			return err
		}

		query := fmt.Sprintf("COMMENT ON COLUMN %s.%s IS '%s'", g.meta.Cfg.TableName, column.Name, string(cm))
		log.Info("execute query: %s", query)
		_, err = conn.ExecContext(g.ctx, query)
		if err != nil {
			return err
		}
	}
	// add comment on ext column
	if g.meta.Table.ExtColumn != nil {
		valueRange := g.meta.Table.ExtColumn.GetValueRange()
		if valueRange != nil {
			//g.meta.Table.ExtColumn.Comment = valueRange.ToJSON()
			log.Info("[Generator.TELEMATICS] ext column: %s, type: %s, comment: %s, value range: %+v", g.meta.Table.ExtColumn.Name, g.meta.Table.ExtColumn.TypeName, g.meta.Table.ExtColumn.Comment, valueRange)
		}
	}
	return nil
}

func (g *Generator) Close() error {
	g.cancelFunc()
	g.wg.Wait()

	log.Verbose("[Generator.TELEMATICS] Gen time: %s", time.Duration(accGenTime))
	log.Verbose("[Generator.TELEMATICS] Write time: %s", time.Duration(accWriteTime))
	log.Verbose("[Generator.TELEMATICS] Misc time1: %s", time.Duration(accMiscTime1))
	log.Verbose("[Generator.TELEMATICS] Write Cnt: %d", accWriteCnt)
	log.Verbose("[Generator.TELEMATICS] Acc Size: %d", accWriteSize)
	log.Verbose("[Generator.TELEMATICS] Max Size: %d", maxWriteSize)

	return nil
}

func (g *Generator) CreatePluginConfig() interface{} {
	return &Config{}
}

func (g *Generator) GetDefaultFlags() (*pflag.FlagSet, interface{}) {
	gCfg := &Config{}
	p := pflag.NewFlagSet("generator.telematics", pflag.ContinueOnError)
	p.IntVar(&gCfg.DisorderRatio, "generator-disorder-ratio", 0, "The percent of data timestamp that is disordered.\n"+
		"Expected to be an integer ranging from 0 to 100 (included).")
	p.IntVar(&gCfg.BatchSize, "generator-batch-size", 1, "The number of lines of data generated for a tag of a given timestamp.\n"+
		"e.g. It is set to be 5, then for tag \"tag1\" with ts of \"2022-04-02 15:04:03\",\n5 lines of data will be generated and sent into DBMS.\n"+
		"Eventually, however, they will be merged as 1 row in DBMS.")
	p.StringVar(&gCfg.Randomness, "generator-randomness", _RANDOMNESS_LEVEL_OFF, "The randomness of metrics, OFF/S/M/L")
	p.IntVar(&gCfg.EmptyValueRatio, "generator-empty-value-ratio", 90, "the ratio of empty metrics value in one line.\n"+
		"Expected to be an integer ranging from 0 to 100 (included).")

	p.IntVar(&gCfg.NumGoRoutine, "generator-num-goroutine", 1, "num of goroutines that it will use to call write function")
	p.IntVar(&gCfg.WriteBatchSize, "generator-write-batch-size", 4, "the estimated mega bytes of batch size to call write function")

	_ = p.MarkHidden("generator-num-goroutine")
	_ = p.MarkHidden("generator-write-batch-size")
	return p, gCfg
}

var accMiscTime1, accGenTime, accWriteTime, accWriteSize int64
var accWriteCnt, maxWriteSize int

func (g *Generator) generateAndWriteBatch(batches [][]string, tpl [][]string, ts time.Time) error {
	st := time.Now()
	startIdx := ts.Unix() % g.cfg.templateSize
	copy(batches, tpl[startIdx:])
	for i := g.cfg.templateSize - startIdx; i < g.gcfg.TagNum; i += g.cfg.templateSize {
		copy(batches[i:], tpl)
	}
	tt := time.Now()
	accMiscTime1 += tt.Sub(st).Nanoseconds()

	// judge the number of tag num to form a batch
	// TODO: extract to the caller and save the results in generator
	tagIndexRanges := g.calTagIndexesOfBatches(batches)

	var lastIndex int64
	for _, index := range tagIndexRanges {
		// TODO: batchRowSize is not important or necessary
		st = time.Now()
		batchData, batchLines, batchRowSize := g.generateBatch(g.meta.Table.VinValues[lastIndex:index], ts, batches)
		numOfDataBatch := len(batchData)

		tt = time.Now()
		accGenTime += tt.Sub(st).Nanoseconds()

		// log.Info("Gen %d batches for %s: %d~%d\n", len(batchData), ts, lastIndex, index)

		//  numOfDataBatch > 1 means, it should be sprawled in multiple goroutines
		accWriteCnt += numOfDataBatch
		if numOfDataBatch > 1 {
			eg := new(errgroup.Group)
			for i := 0; i < numOfDataBatch; i++ {
				size := len(batchData[i])
				accWriteSize += int64(size)
				if maxWriteSize < size {
					maxWriteSize = size
				}
				iInside := i
				eg.Go(func() error {
					// log.Info("    %d batches len = %d\n", iInside, len(batchData[iInside]))
					// if iInside == 0 {
					// 	log.Info("    %d batches %s\n", iInside, batchData[iInside])
					// }
					return g.writeFunc(batchData[iInside], batchLines[iInside], batchRowSize[iInside])
				})
			}
			if err := eg.Wait(); err != nil {
				return err
			}
		} else {
			size := len(batchData[0])
			accWriteSize += int64(size)
			if maxWriteSize < size {
				maxWriteSize = size
			}
			err := g.writeFunc(batchData[0], batchLines[0], batchRowSize[0])
			if err != nil {
				return err
			}
		}
		accWriteTime += time.Since(tt).Nanoseconds()
		lastIndex = index
	}
	return nil
}

func (g *Generator) writeByTimeRange(tpl [][]string) error {
	startTime := g.gcfg.StartAt
	endTime := g.gcfg.EndAt

	batches := make([][]string, g.gcfg.TagNum)

	for ts := startTime; ts.Before(endTime); ts = ts.Add(time.Second * time.Duration(g.gcfg.TimestampStepInSecond)) {
		select {
		case <-g.ctx.Done():
			return nil
		default:
			if err := g.generateAndWriteBatch(batches, tpl, ts); err != nil {
				return err
			}
		}
	}

	return nil
}

func (g *Generator) writeByRealtime(tpl [][]string) error {
	ticker := time.NewTicker(time.Second * time.Duration(g.gcfg.TimestampStepInSecond))

	batches := make([][]string, g.gcfg.TagNum)

	for {
		select {
		case <-g.ctx.Done():
			return nil
		case ts := <-ticker.C:
			if err := g.generateAndWriteBatch(batches, tpl, ts); err != nil {
				return err
			}
		}
	}
}

func (g *Generator) write(tpl [][]string) (err error) {
	defer log.Info("[Generator.TELEMATICS] Data generating ended")
	if g.gcfg.IsRealtimeMode {
		err = g.writeByRealtime(tpl)
	} else {
		err = g.writeByTimeRange(tpl)
	}

	return err
}

func (g *Generator) genTpl() ([][]string, error) {
	table := g.meta.Table
	mocker, err := mxmock.NewMXMockerFromColumns(table.Columns)
	if err != nil {
		return nil, err
	}

	// exclude timestamp columns and vin columns,
	// because they are not to be generated by mxmock
	mocker.ExcludeColumn(table.ColumnNameTS, table.ColumnNameVIN)

	// batchSize is how many non-null metrics are there in a tuple in DBMS,
	// which might be inserted/merged in multiple rows
	batchSize := (100 - g.cfg.emptyValueRatio) *
		int(table.TotalMetricsCount) / 100

	tpl := make([][]string, 0, g.cfg.templateSize)
	for s := int64(0); s < g.cfg.templateSize; s++ {
		rand.Seed(time.Now().UnixNano())

		rows, err := mocker.MockBatchWithTotalValues(g.cfg.batchLine, batchSize)
		if err != nil {
			return nil, err
		}

		result := make([]string, len(rows))
		for idx, row := range rows {
			buff := bytes.NewBuffer(nil)
			buff.WriteString(strings.Join(row, util.DELIMITER))
			result[idx] = buff.String()
		}

		tpl = append(tpl, result)
	}
	return tpl, nil
}

func (g *Generator) validate() error {
	if int64(g.cfg.BatchSize) > g.meta.Table.TotalMetricsCount {
		return mxerror.CommonErrorf(
			"batch size(%d) is greater than metrics number(%d)",
			g.cfg.BatchSize,
			g.meta.Table.TotalMetricsCount,
		)
	}
	return nil
}

func (g *Generator) IsNil() bool {
	return g == nil
}

// calTagIndexesOfOneBatch will take the number of tags: tagNum
// and actual rows to writer for each tag in a certain timestamp: rows
// according to generator's config of batch data size to write at once
// generate a slice of tag boundaries (index 0 is not included)
// For example:
// example 1:
// rows: []string{"1|2|||", "||3|4|", "||||5"}, 6 bytes each row, 18 bytes in total
// g.cfg.WriteBatchSize: 9000 bytes
// g.cfg.TagNum: 10,000
// then: it will return a slice of []int64{5000, 10000}
// P.S.: Despite that index 0 is not included, it will be taken into the calculation next.
// tags with id of 0,1,2,3...4999 will be in the first batch to write (may be in multiple goroutines)
// so that the total size of the first batch is 5000 * 18 = 9000 bytes
// tags with id of 5000,5001...9999 will be in the second batch to write (may be in multiple goroutines)
// example 2:
// rows: []string{"1|2|3|"}, 6 bytes per tag per timestamp
// g.cfg.WriteBatchSize: 7 bytes
// g.cfg.TagNum: 10
// in this situation, actual write batch size will be 6 * 2 = 12,
// for rows per tag per timestamp could not be furthermore separated
// thus it will return: []in64{2, 4, 6, 8, 10}
// P.S.: g.cfg.WriteBatchSize is a multiple of 1024*1024 bytes in real world,
// and the examples above is to let the reader understand the mechanism.
func (g *Generator) calTagIndexesOfBatches(batches [][]string) []int64 {
	writeBatchSizeInBytes := int64(g.cfg.WriteBatchSize) * _MEGA_BYTES
	tagIndexes := make([]int64, 0)

	restBytesInABuffer := writeBatchSizeInBytes
	for idx, batch := range batches {
		var numOfBytesBatch int64
		for _, row := range batch {
			numOfBytesBatch += int64(len(row))
		}

		restBytesInABuffer -= numOfBytesBatch
		if restBytesInABuffer <= 0 {
			tagIndexes = append(tagIndexes, int64(idx)+1) // not including end boundary
			if idx < len(batches)-1 {
				restBytesInABuffer = writeBatchSizeInBytes
			}

			continue
		}
	}
	if restBytesInABuffer > 0 {
		// there are still space left, then we grow the writeBatchSize to accommodate another one tag's data
		tagIndexes = append(tagIndexes, int64(len(batches)))
	}

	return tagIndexes
}

// generateBatch is used to produce batches that each will be sent in one goroutine
// input:
// 1. vins: the range of device id to be generated.
// 2. ts: timestamp of these data
// 3. batch: data for a certain ts and certain tag. len(batch) will be 1 if upsert will not be included in the config.
// output:
// three slices, the length of those slices should be g.cfg.NumGoRoutine respectively.
// 1. []string: each element is the data to be sent in one single goroutine.
// 2. []int64: Correspondingly, each element is the number of lines that each element of the string slice will include.
// 3. []int64: Correspondingly, each element is the size (in bytes) of each element of the string slice.
func (g *Generator) generateBatch(vins []string, ts time.Time, batches [][]string) ([][]byte, []int64, []int64) {
	numOfBatch := g.cfg.NumGoRoutine
	batchData := make([][]byte, numOfBatch)
	batchDataLines := make([]int64, numOfBatch)
	batchDataSize := make([]int64, numOfBatch)

	numOfTag := len(vins)
	tagsPerBatch := numOfTag / numOfBatch
	if numOfTag%numOfBatch > 0 {
		// make sure that $numOfBatch with $tagsPerBatch (the last batch may noy be full)
		// will cover all of tags
		tagsPerBatch++
	}
	tagBounds := make([]int, 0, numOfBatch)
	for i := tagsPerBatch; ; i += tagsPerBatch {
		if i >= numOfTag {
			tagBounds = append(tagBounds, numOfTag)
			break
		}
		tagBounds = append(tagBounds, i)
	}

	tsString := ts.Format(util.TIME_FMT)
	tsOutOfOrderString := ts.Add(-g.cfg.outOrderDuration).Format(util.TIME_FMT)
	lastIndex := 0

	var wg sync.WaitGroup
	for idx, tagBound := range tagBounds {
		wg.Add(1)
		go func(idx, tagBound, lastIndex int) {
			defer wg.Done()

			var lines int64
			buffer := g.cacheBuff[idx]
			buffer.Reset()

			for i := lastIndex; i < tagBound; i++ {
				ts := tsString
				if g.cfg.percentOfOutOrder > 0 && rand.Intn(100) < g.cfg.percentOfOutOrder {
					ts = tsOutOfOrderString
				}
				for _, row := range batches[i] {
					buffer.WriteString(ts)
					buffer.WriteString(util.DELIMITER)
					buffer.WriteString(vins[i])
					buffer.WriteString(util.DELIMITER)
					buffer.WriteString(row)
					buffer.WriteByte('\n')
					lines++
				}
			}
			batchData[idx] = buffer.Bytes()
			batchDataLines[idx] = lines
			batchDataSize[idx] = int64(buffer.Len())

		}(idx, tagBound, lastIndex)

		lastIndex = tagBound
	}
	wg.Wait()
	return batchData, batchDataLines, batchDataSize
}
