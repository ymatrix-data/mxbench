package telematics

import (
	"context"
	"fmt"
	"time"

	"github.com/spf13/pflag"

	"github.com/ymatrix-data/mxbench/internal/engine"
	"github.com/ymatrix-data/mxbench/internal/engine/metadata"
	"github.com/ymatrix-data/mxbench/internal/util/log"
)

type Config struct {
	Parallel           []int    `mapstructure:"benchmark-parallel"`
	RunQueryNames      []string `mapstructure:"benchmark-run-query-names"`
	CombinationQueries string   `mapstructure:"benchmark-combination-queries"`
	CustomQueries      []string `mapstructure:"benchmark-custom-queries"`
	RunTimes           int64    `mapstructure:"benchmark-run-times"`
	RunTimeInSecond    uint64   `mapstructure:"benchmark-runtime-in-second"`
	ProgressFormat     string   `mapstructure:"benchmark-progress-format"`

	// hidden
	TimestampStart     string `mapstructure:"benchmark-ts-start"`
	TimestampEnd       string `mapstructure:"benchmark-ts-end"`
	SimpleMetricsCount int64  `mapstructure:"benchmark-simple-metrics-count"`
	JSONMetricsCount   int64  `mapstructure:"benchmark-json-metrics-count"`

	NumOfParsedCombinationQueries int
}

type Benchmark struct {
	ctx        context.Context
	cancelFunc context.CancelFunc
	stat       *Stat
	cfg        *Config
	bcfg       engine.BenchmarkConfig
	gcfg       engine.Config
	meta       *metadata.Metadata
	execFunc   engine.ExecBenchFunc

	writerFinCh <-chan error
}

func NewBenchmark(cfg engine.BenchmarkConfig) engine.IBenchmark {
	bCfg := cfg.PluginConfig.(*Config)
	ctx, cancelFunc := context.WithCancel(context.Background())
	return &Benchmark{
		bcfg: cfg, cfg: bCfg,
		ctx: ctx, cancelFunc: cancelFunc,
		stat: newStat(bCfg)}
}

func (b *Benchmark) Run(writerFinCh <-chan error, cfg engine.Config, meta *metadata.Metadata, execFunc engine.ExecBenchFunc) error {
	b.gcfg = cfg
	b.meta = meta
	b.execFunc = execFunc
	b.writerFinCh = writerFinCh
	// We are assuming
	// 1. the first ${len(b.cfg.RunQueryNames)} are named queries
	queries := b.newNamedQueries()
	// 2. the following ${b.cfg.NumberOfCombinationQueries} are
	queries = append(queries, b.newCombinationQueries()...)
	// 3. the last ${len(b.cfg.CustomQueries)} are custom queries
	// The order and the numbers are important in Report Presentation of Benchmark Stat.
	return b.exec(
		append(queries, b.newCustomizedQueries()...))
}

func (b *Benchmark) Close() error {
	b.cancelFunc()
	return nil
}

func (b *Benchmark) GetStat() engine.Stat {
	return b.stat
}

func (b *Benchmark) CreatePluginConfig() interface{} {
	return &Config{}
}

func (b *Benchmark) GetDefaultFlags() (*pflag.FlagSet, interface{}) {
	sCfg := &Config{}
	p := pflag.NewFlagSet("benchmark.telematics", pflag.ContinueOnError)
	p.IntSliceVar(&sCfg.Parallel, "benchmark-parallel", nil, "parallels of benchmark,"+
		"use \",\" to run queries with different concurrency.\n"+
		"For example, input [1, 8] to run queries with parallel of 1 and 8 respectively.")
	p.StringSliceVar(&sCfg.RunQueryNames, "benchmark-run-query-names",
		nil,
		"query names to be run, the default includes none of telematics benchmark queries.\n"+
			"Please input the query names, and use \",\" to separate query names.\n"+
			fmt.Sprintf(
				"For example, input:\n%s\nto run all telematics queries, or a subset of it.\n"+
					"Any other arbitrary query name will be skipped.\n"+
					"The order matters.",
				getQueryNamesInConfigFile()))
	p.StringSliceVar(&sCfg.CustomQueries, "benchmark-custom-queries",
		nil,
		"custom query SQLs, use \",\" to separate query statements.\n"+
			"For example, [\"SELECT COUNT(*) from t1\", \"SELECT MAX(ts) from t1\"]")
	p.StringVar(&sCfg.CombinationQueries, "benchmark-combination-queries", "", "Queries by combing expressions")
	p.Int64Var(&sCfg.RunTimes, "benchmark-run-times", 0, "the times of queries with set parallels")
	p.Uint64Var(&sCfg.RunTimeInSecond, "benchmark-runtime-in-second", 60, "total runtime of queries, only take effect when benchmark-run-times is 0")
	p.StringVar(&sCfg.ProgressFormat, "benchmark-progress-format", "list", "progress format. support \"list\", \"json\"")

	// hidden
	p.StringVar(&sCfg.TimestampStart, "benchmark-ts-start", "", "the start timestamp of query")
	p.StringVar(&sCfg.TimestampEnd, "benchmark-ts-end", "", "the end timestamp of query")
	p.Int64Var(&sCfg.SimpleMetricsCount, "benchmark-simple-metrics-count", 290, "the count of metrics of simple type, max is 998")
	p.Int64Var(&sCfg.JSONMetricsCount, "benchmark-json-metrics-count", 10, "the count of metrics of json type, max is 1600")

	_ = p.MarkHidden("benchmark-ts-start")
	_ = p.MarkHidden("benchmark-ts-end")
	_ = p.MarkHidden("benchmark-simple-metrics-count")
	_ = p.MarkHidden("benchmark-json-metrics-count")

	return p, sCfg
}

func (b *Benchmark) IsNil() bool {
	return b == nil
}

func (b *Benchmark) exec(queries []engine.Query) error {
	opt := engine.ExecBenchOption{
		RunTimes: b.cfg.RunTimes,
		Duration: time.Second * time.Duration(b.cfg.RunTimeInSecond)}

	queriesNum := len(queries)
	if queriesNum == 0 || len(b.cfg.Parallel) == 0 {
		log.Info("No queries need to be run, exiting benchmark")
		return nil
	}
	executedRuns := 0
	for {
		// if the writer has finished,
		// and benchmark has finished at least once
		// stop loop
		select {
		case <-b.writerFinCh:
			if executedRuns > 0 {
				return nil
			}
		case <-b.ctx.Done():
			return nil
		default:
		}
		log.Info("Begin to execute no. %d run queries with configured parallels", executedRuns+1)
		b.stat.Reset()
		for _, p := range b.cfg.Parallel {
			opt.Parallel = p
			log.Info("Begin to exec queries with parallel %d", p)
			for i, q := range queries {
				select {
				case <-b.ctx.Done():
					log.Info("Benchmark canceled before executing query %d of %d: %s", i+1, queriesNum, q.GetName())
					return nil
				default:
				}
				log.Info("Begin to exec query %d of %d: %s", i+1, queriesNum, q.GetName())

				var err error
				execStat := engine.NewExecBenchStat(opt, q)
				b.stat.AddSubStat(execStat)
				err = b.execFunc(b.ctx, q, execStat)

				log.Info("Query %d of %d: %s done", i+1, queriesNum, q.GetName())
				fmt.Printf("Sub Stat for query %d of %d: %s done\n%s\n", i+1, queriesNum, q.GetName(),
					b.stat.subStats[len(b.stat.subStats)-1].GetSummary())
				if err != nil {
					return err
				}
			}
		}
		executedRuns++
	}
}

func (b *Benchmark) newNamedQueries() []engine.Query {
	queries := make([]engine.Query, 0)
	for _, queryName := range b.cfg.RunQueryNames {
		newFunc, ok := queryNameNewFunc[queryName]
		if !ok {
			log.Warn("query name: %s is not included in %s benchmark, skipping...", queryName, b.bcfg.Plugin)
			continue
		}
		queries = append(queries, newFunc(b.meta, b.cfg))
	}

	return queries
}

func (b *Benchmark) newCombinationQueries() []engine.Query {
	queries, err := parseCombinationQueries(b.cfg.CombinationQueries)
	if err != nil {
		log.Warn("error occurs when compiling benchmark-combination-queries: %v", err)
	}
	// back report to b.cfg of the number of combination queries parsed
	b.cfg.NumOfParsedCombinationQueries = len(queries)
	log.Info("%d of combination queries have been successfully parsed", b.cfg.NumOfParsedCombinationQueries)
	return transformCombinationQueries(queries, b.meta, b.cfg)
}

func (b *Benchmark) newCustomizedQueries() []engine.Query {
	queries := make([]engine.Query, 0, len(b.cfg.CustomQueries))
	for i, query := range b.cfg.CustomQueries {
		queries = append(queries, newQueryCustom(query, i+1))
	}
	return queries
}
