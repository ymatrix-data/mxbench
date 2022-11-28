package injectors

import "github.com/ymatrix-data/mxbench/internal/engine"

// Raise a panic before engine runs
type QuickPanicInjector struct {
	SkipConnInjector
}

func (j *QuickPanicInjector) PreEngineRun(*engine.Config) {
	panic("artificial panic")
}
