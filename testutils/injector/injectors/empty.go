package injectors

import "github.com/ymatrix-data/mxbench/internal/engine"

type EmptyInjector struct{}

func (j *EmptyInjector) PreEngineRun(cfg *engine.Config)  {}
func (j *EmptyInjector) PostEngineRun(cfg *engine.Config) {}
func (j *EmptyInjector) PostEngineClose()                 {}
