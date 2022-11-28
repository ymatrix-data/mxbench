package injectors

import (
	"github.com/ymatrix-data/mxbench/internal/engine"
)

// Skip database connection on startup
type SkipConnInjector struct {
	EmptyInjector
}

func (j *SkipConnInjector) PreEngineRun(cfg *engine.Config) {
	// TODO: Hijack DB operations
}
