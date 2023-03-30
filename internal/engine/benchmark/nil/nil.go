package nil

import (
	"github.com/spf13/pflag"

	"github.com/ymatrix-data/mxbench/internal/engine"
	"github.com/ymatrix-data/mxbench/internal/engine/metadata"
)

type Config struct{}

type Stat struct{}

func newStat() *Stat {
	return &Stat{}
}

func (s *Stat) AddSubStat(engine.Stat) {}

func (s *Stat) GetSummary() string {
	return ""
}

func (s *Stat) GetFormattedSummary(string) string {
	return ""
}

func (s *Stat) GetCurrentProgress(_ ...interface{}) map[string]interface{} {
	return nil
}

func (s *Stat) GetProgress() string {
	return ""
}

func (s *Stat) GetSubStats() []engine.Stat {
	return nil
}
func (s *Stat) GetName() string {
	return ""
}

type Benchmark struct {
	cfg  engine.BenchmarkConfig
	stat *Stat
}

func NewBenchmark(cfg engine.BenchmarkConfig) engine.IBenchmark {
	return &Benchmark{cfg: cfg}
}

func (b *Benchmark) Run(writerFinCh <-chan error, cfg engine.Config, meta *metadata.Metadata, exec engine.ExecBenchFunc) error {
	b.stat = newStat()
	return nil
}

func (b *Benchmark) Close() error {
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
	p := pflag.NewFlagSet("benchmark.nil", pflag.ContinueOnError)
	return p, sCfg
}

func (b *Benchmark) IsNil() bool {
	return b == nil
}
