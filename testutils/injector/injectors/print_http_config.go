package injectors

import (
	"fmt"
	"os"

	"github.com/ymatrix-data/mxbench/internal/engine"
	"github.com/ymatrix-data/mxbench/internal/engine/writer/http"
)

// Print HTTP configuration
type PrintHTTPCfgInjector struct {
	EmptyInjector
}

func (j *PrintHTTPCfgInjector) PreEngineRun(cfg *engine.Config) {
	if cfg.WriterCfg.PluginConfig == nil {
		fmt.Fprintf(os.Stderr, "No writer config")
		os.Exit(0)
	}
	pCfg, ok := cfg.WriterCfg.PluginConfig.(*http.Config)
	if !ok {
		fmt.Fprintf(os.Stderr, "Writer not http")
		os.Exit(0)
	}
	fmt.Fprintf(os.Stderr, "%+v\n", pCfg)
	os.Exit(0)
}
