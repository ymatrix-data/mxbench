package nil

import (
	"github.com/spf13/pflag"

	"github.com/ymatrix-data/mxbench/internal/engine"
)

type Config struct{}

type Writer struct{}

type Stat struct{}

func (s *Stat) AddSubStat(engine.Stat) {}

func (s *Stat) GetSummary() string {
	return ""
}

func (s *Stat) GetFormattedSummary() string {
	return ""
}

func (s *Stat) GetProgress() string {
	return ""
}

func (s *Stat) GetCurrentProgress(_ ...interface{}) map[string]interface{} {
	return nil
}

func (s *Stat) GetSubStats() []engine.Stat {
	return nil
}
func (s *Stat) GetName() string {
	return ""
}

func NewWriter(cfg engine.WriterConfig) engine.IWriter {
	return &Writer{}
}

func (w *Writer) Start(cfg engine.Config, _ engine.VolumeDesc) (<-chan error, error) {
	finCh := make(chan error)
	close(finCh)
	return finCh, nil
}

func (w *Writer) Stop() error {
	return nil
}

func (w *Writer) Write(msg []byte, _, _ int64) error {
	return nil
}

func (w *Writer) WriteEOF() error {
	return nil
}

func (w *Writer) GetStat() engine.Stat {
	return &Stat{}
}

func (w *Writer) CreatePluginConfig() interface{} {
	return &Config{}
}

func (w *Writer) GetDefaultFlags() (*pflag.FlagSet, interface{}) {
	hCfg := &Config{}
	p := pflag.NewFlagSet("writer.nil", pflag.ContinueOnError)
	return p, hCfg
}

func (w *Writer) IsNil() bool {
	return w == nil
}
