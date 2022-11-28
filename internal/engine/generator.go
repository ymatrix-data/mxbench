package engine

import (
	"github.com/ymatrix-data/mxbench/internal/engine/metadata"
)

type IGenerator interface {
	NilCheck
	ConfigurablePlugin
	GetPrediction(*metadata.Table) (GeneratorPrediction, error)
	ModifyMetadataConfig(*metadata.Config)
	Run(GlobalConfig, *metadata.Metadata, WriteFunc) error
	Close() error
}

type GeneratorConfig struct {
	Plugin string `mapstructure:"generator"`

	// Plugin specific config
	PluginConfig interface{}

	// derived from elsewhere
	GlobalConfig *GlobalConfig
}

type GetTableSizeFunc func() (int64, error)

type GeneratorPrediction struct {
	Count, Size int64
}

type VolumeDesc struct {
	GeneratorPrediction GeneratorPrediction
	GetTableSizeFunc    GetTableSizeFunc
}
