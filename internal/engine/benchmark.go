package engine

import (
	"context"
	"time"

	"github.com/ymatrix-data/mxbench/internal/engine/metadata"
)

type ExecBenchOption struct {
	Parallel int
	RunTimes int64
	Duration time.Duration
}

type Query interface {
	GetSQL() string
	GetName() string
}

type ExecBenchFunc func(context.Context, Query, Stat) error

type IBenchmark interface {
	NilCheck
	ConfigurablePlugin
	Run(<-chan error, Config, *metadata.Metadata, ExecBenchFunc) error
	Close() error
	GetStat() Stat
}

type BenchmarkConfig struct {
	Plugin string `mapstructure:"benchmark"`

	// Plugin specific config
	PluginConfig interface{}
}
