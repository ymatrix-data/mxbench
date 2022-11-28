package engine

type WriteFunc func([]byte, int64, int64) error

type IWriter interface {
	NilCheck
	ConfigurablePlugin
	// Close finCh after writer ended.
	Start(Config, VolumeDesc) (finCh <-chan error, err error)
	Stop() error
	Write([]byte, int64, int64) error
	WriteEOF() error
	GetStat() Stat
}

type WriterConfig struct {
	Plugin string `mapstructure:"writer"`

	// Plugin specific config
	PluginConfig interface{}
}
