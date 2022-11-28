package injectors

import (
	"fmt"
	"os"

	"github.com/ymatrix-data/mxbench/internal/engine"
)

// Print generator details
type PrintGeneratorInjector struct {
	SkipConnInjector
}

func (j *PrintGeneratorInjector) PreEngineRun(cfg *engine.Config) {
	fmt.Fprintf(os.Stderr, "Generator instance is %s\n", cfg.GeneratorCfg.Plugin)
	fmt.Fprintf(os.Stderr, "PluginConfig is %+v\n", cfg.GeneratorCfg.PluginConfig)
	os.Exit(0)
}
