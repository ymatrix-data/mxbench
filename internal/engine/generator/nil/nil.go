package nil

import (
	"github.com/spf13/pflag"

	"github.com/ymatrix-data/mxbench/internal/engine"
	"github.com/ymatrix-data/mxbench/internal/engine/metadata"
	"github.com/ymatrix-data/mxbench/internal/util/log"
)

type Config struct {
}

type Generator struct {
}

func NewGenerator(cfg engine.GeneratorConfig) engine.IGenerator {
	return &Generator{}
}

func (g *Generator) Run(cfg engine.GlobalConfig, meta *metadata.Metadata, writeFunc engine.WriteFunc) error {
	log.Info("[Generator.Nil] Nil Generator started")
	log.Info("[Generator.Nil] Nil Generator ended")
	return nil
}

func (g *Generator) GetPrediction(_ *metadata.Table) (engine.GeneratorPrediction, error) {
	return engine.GeneratorPrediction{}, nil
}

func (g *Generator) ModifyMetadataConfig(_ *metadata.Config) {}

func (g *Generator) Close() error {
	return nil
}

func (g *Generator) CreatePluginConfig() interface{} {
	return &Config{}
}

func (g *Generator) GetDefaultFlags() (*pflag.FlagSet, interface{}) {
	gCfg := &Config{}
	p := pflag.NewFlagSet("generator.nil", pflag.ContinueOnError)
	return p, gCfg
}

func (g *Generator) IsNil() bool {
	return g == nil
}
