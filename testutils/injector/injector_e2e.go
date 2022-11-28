//go:build e2e
// +build e2e

package injector

/* THIS FILE IS ONLY COMPILED IN END-TO-END TEST */

import (
	"fmt"
	"os"

	"github.com/ymatrix-data/mxbench/internal/engine"
	"github.com/ymatrix-data/mxbench/testutils/injector/injectors"
)

var injectorIn faultInjector

// Fault Injector is a framework to hook into mxbench
// It can be used to
//   - simulate crash
//   - hijack internal data struct
// Compose your own injector by implementing FaultInjector interface

type faultInjector interface {
	PreEngineRun(cfg *engine.Config)
	PostEngineRun(cfg *engine.Config)
	PostEngineClose()
}

func init() {
	var injectorName string
	var nextIsInjector bool

	// Extract injector name from arguments
	for _, key := range os.Args {
		if nextIsInjector {
			injectorName = key
			break
		}
		if key == "--injector" {
			nextIsInjector = true
		} else if len(key) > 11 && key[:11] == "--injector=" {
			injectorName = key[11:]
			break
		}
	}

	// hook into main
	if injectorName != "" {
		injectorIn = getInjectorByName(injectorName)
		fmt.Printf("Apply FaultInjector: %s, the instance at %p\n", injectorName, injectorIn)
	}
}

func getInjectorByName(name string) faultInjector {
	switch name {
	case "print_db":
		return &injectors.PrintDBInjector{}
	case "print_generator":
		return &injectors.PrintGeneratorInjector{}
	case "quit_hang":
		return &injectors.QuitHangInjector{}
	case "error3s":
		return &injectors.Error3sInjector{}
	case "panic3s":
		return &injectors.Panic3sInjector{}
	case "skip_conn":
		return &injectors.SkipConnInjector{}
	case "quick_panic":
		return &injectors.QuickPanicInjector{}
	case "log5s":
		return &injectors.Log5sInjector{}
	case "print_http_config":
		return &injectors.PrintHTTPCfgInjector{}
	case "print_misc":
		return &injectors.PrintMiscInjector{}
	}
	return nil
}

// PreEngineRun happens just before engine runs
// All config from CLI is parsed
func PreEngineRun(cfg *engine.Config) {
	if injectorIn != nil {
		injectorIn.PreEngineRun(cfg)
	}
}

// PostEngineRun happens when engine full run and signal been setup
// This hook point is to simulate exception during normal loading
func PostEngineRun(cfg *engine.Config) {
	if injectorIn != nil {
		injectorIn.PostEngineRun(cfg)
	}
}

// PostEngineClose happens on engine close done with grace
// This hook point is to simulate graceful shutdown hang
func PostEngineClose() {
	if injectorIn != nil {
		injectorIn.PostEngineClose()
	}
}
