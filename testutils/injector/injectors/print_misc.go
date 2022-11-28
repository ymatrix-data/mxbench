package injectors

import (
	"fmt"
	"os"

	"github.com/ymatrix-data/mxbench/internal/engine"
)

// Print misc details
type PrintMiscInjector struct {
	SkipConnInjector
}

func (j *PrintMiscInjector) PreEngineRun(cfg *engine.Config) {
	fmt.Fprintf(os.Stderr, "%+v\n", cfg.GlobalCfg)
	os.Exit(0)
}
