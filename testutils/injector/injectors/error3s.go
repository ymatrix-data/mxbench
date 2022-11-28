package injectors

import (
	"os"
	"time"

	"github.com/ymatrix-data/mxbench/internal/engine"
	"github.com/ymatrix-data/mxbench/internal/util/log"
)

// Raise an error log after engine runs for 10s
type Error3sInjector struct {
	SkipConnInjector
}

func (j *Error3sInjector) PostEngineRun(*engine.Config) {
	go func() {
		var i int
		ticker := time.NewTicker(time.Second)
		defer ticker.Stop()
		for {
			<-ticker.C
			log.Info("%s", time.Now())
			i++
			if i == 3 {
				log.Error("This is an error") // Print an error with log
				log.Info("This is an info")   // Print an info  with log
			}
			if i >= 4 {
				os.Exit(0)
			}
		}
	}()
}
