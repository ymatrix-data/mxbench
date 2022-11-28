package injectors

import (
	"time"

	"github.com/ymatrix-data/mxbench/internal/engine"
	"github.com/ymatrix-data/mxbench/internal/util/log"
)

// Raise a panic after Engine runs for 10s
type Panic3sInjector struct {
	SkipConnInjector
}

func (j *Panic3sInjector) PostEngineRun(*engine.Config) {
	go func() {
		var i int
		ticker := time.NewTicker(time.Second)
		defer ticker.Stop()
		for {
			<-ticker.C
			log.Info("%s", time.Now())
			i++
			if i > 3 {
				panic("artificial panic")
			}
		}
	}()
}
