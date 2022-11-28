//go:build !e2e
// +build !e2e

package injector

import "github.com/ymatrix-data/mxbench/internal/engine"

// PreEngineRun happens just before engine runs
// All config from CLI is parsed
func PreEngineRun(cfg *engine.Config) {}

// PostEngineRun happens when engine full run and signal been setup
// This hook point is to simulate exception during normal loading
func PostEngineRun(cfg *engine.Config) {}

// PostEngineClose happens on engine close done with grace
// This hook point is to simulate graceful shutdown hang
func PostEngineClose() {}
