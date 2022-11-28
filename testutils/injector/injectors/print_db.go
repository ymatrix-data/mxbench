package injectors

import (
	"fmt"
	"os"

	"github.com/ymatrix-data/mxbench/internal/engine"
)

// Print db info
type PrintDBInjector struct {
	SkipConnInjector
}

func (j *PrintDBInjector) PreEngineRun(cfg *engine.Config) {
	fmt.Fprintf(os.Stderr, "Database is %s\n", cfg.DB.Database)
	os.Exit(0)
}
