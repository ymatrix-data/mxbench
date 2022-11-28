package telematics

import (
	"time"

	"github.com/ymatrix-data/mxbench/internal/engine/metadata"
)

const (
	//TODO:
	_TEMPLATE_SIZE_SMALL  = 20 * 60
	_TEMPLATE_SIZE_MEDIUM = 60 * 60
	_TEMPLATE_SIZE_LARGE  = 2 * 60 * 60
	_MEGA_BYTES           = 1024 * 1024
)

type RandomnessLevel = string

const (
	_RANDOMNESS_LEVEL_OFF    RandomnessLevel = "OFF"
	_RANDOMNESS_LEVEL_SMALL  RandomnessLevel = "S"
	_RANDOMNESS_LEVEL_MEDIUM RandomnessLevel = "M"
	_RANDOMNESS_LEVEL_LARGE  RandomnessLevel = "L"
)

type Config struct {
	DisorderRatio   int             `mapstructure:"generator-disorder-ratio"`
	BatchSize       int             `mapstructure:"generator-batch-size"`
	EmptyValueRatio int             `mapstructure:"generator-empty-value-ratio"`
	Randomness      RandomnessLevel `mapstructure:"generator-randomness"`
	WriteBatchSize  int             `mapstructure:"generator-write-batch-size"`
	NumGoRoutine    int             `mapstructure:"generator-num-goroutine"`

	templateSize      int64
	percentOfOutOrder int
	batchLine         int
	emptyValueRatio   int
	outOrderDuration  time.Duration
}

func (cfg *Config) init() {
	cfg.templateSize = cfg.getTemplateSize(cfg.Randomness)
	cfg.percentOfOutOrder = cfg.DisorderRatio
	cfg.outOrderDuration = metadata.OutOrderDuration
	cfg.batchLine = cfg.BatchSize
	cfg.emptyValueRatio = cfg.EmptyValueRatio
}

func (cfg *Config) getTemplateSize(randomness RandomnessLevel) int64 {
	switch randomness {
	case _RANDOMNESS_LEVEL_SMALL:
		return _TEMPLATE_SIZE_SMALL
	case _RANDOMNESS_LEVEL_MEDIUM:
		return _TEMPLATE_SIZE_MEDIUM
	case _RANDOMNESS_LEVEL_LARGE:
		return _TEMPLATE_SIZE_LARGE
	default:
		return int64(1)
	}
}
