package injectors

import "time"

// Raise a panic after engine runs for 10s
type QuitHangInjector struct {
	SkipConnInjector
}

func (j *QuitHangInjector) PostEngineStop() {
	time.Sleep(time.Minute)
}
