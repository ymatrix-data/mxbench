package file

import (
	"github.com/spf13/pflag"

	"github.com/ymatrix-data/mxbench/internal/engine"
	"os"
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

func (w *Writer) Write(m []byte, cnt, size int64) error {
	// Get the filename from the environment variable
    filename := os.Getenv("FILENAME")
    if filename == "" {
        filename = "data.csv" // default filename if the environment variable is not set
    }

	// Open the file in append mode, create it if it doesn't exist
    file, err := os.OpenFile(filename, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
    if err != nil {
        return err
    }
    defer file.Close()

    // Write the data to the file
    if _, err := file.Write(m); err != nil {
        return err
    }

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
