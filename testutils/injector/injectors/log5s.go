package injectors

import (
	"os"
	"time"

	"github.com/ymatrix-data/mxbench/internal/engine"
	"github.com/ymatrix-data/mxbench/internal/util/log"
)

// Print log every second.
type Log5sInjector struct {
	EmptyInjector
}

func (j *Log5sInjector) PreEngineRun(cfg *engine.Config) {
	// TODO: Hijack DB operations
}

func (j *Log5sInjector) PostEngineRun(*engine.Config) {
	go func() {
		i := 0
		for {
			i++
			time.Sleep(time.Second)
			log.Info("mock log")
			if i == 5 {
				os.Exit(0)
			}
		}
	}()
}
