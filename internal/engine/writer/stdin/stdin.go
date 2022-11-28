package stdin

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"os/exec"
	"strings"
	"sync"
	"time"

	"github.com/spf13/pflag"

	"github.com/ymatrix-data/mxbench/internal/engine"
	"github.com/ymatrix-data/mxbench/internal/util"
	"github.com/ymatrix-data/mxbench/internal/util/mxerror"
)

type Config struct {
	StreamPrepared           int    `mapstructure:"writer-stream-prepared"`
	Interval                 int    `mapstructure:"writer-interval"`
	mxgatePath               string `mapstructure:"writer-mxgate-path"`
	ProgressFormat           string `mapstructure:"writer-progress-format"`
	ProgressIncludeTableSize bool   `mapstructure:"writer-progress-include-table-size"`
	ProgressWithTimezone     bool   `mapstructure:"writer-progress-with-timezone"`
}

func (c *Config) getProgressTimeLayout() string {
	if c.ProgressWithTimezone {
		return util.TIME_WITH_TZ_FMT
	}
	return util.TIME_FMT
}

type Flags map[string]interface{}

func (fs Flags) ToStr() string {
	vars := []string{}
	for k, v := range fs {
		vars = append(vars, fmt.Sprintf("%s %v", k, v))
	}
	return strings.Join(vars, " ")
}

type Writer struct {
	finCh chan error
	cfg   engine.WriterConfig
	sCfg  *Config

	ctx        context.Context
	cancelFunc context.CancelFunc

	stat *Stat

	stdin  io.WriteCloser
	stdout *bytes.Buffer
	stderr *bytes.Buffer

	globalWG sync.WaitGroup
}

func NewWriter(cfg engine.WriterConfig) engine.IWriter {
	sCfg := cfg.PluginConfig.(*Config)
	ctx, cancelFunc := context.WithCancel(context.Background())
	return &Writer{
		cfg:        cfg,
		sCfg:       sCfg,
		ctx:        ctx,
		cancelFunc: cancelFunc,
		finCh:      make(chan error, 1),
		stat:       &Stat{},
	}
}

func newStat(volumeDesc engine.VolumeDesc, cfg *Config) *Stat {
	return &Stat{volumeDesc: volumeDesc, config: cfg}
}

func (w *Writer) Start(cfg engine.Config, volumeDesc engine.VolumeDesc) (<-chan error, error) {
	w.stat = newStat(volumeDesc, w.sCfg)

	var err error
	var startWG sync.WaitGroup

	startWG.Add(1)
	w.globalWG.Add(1)

	go func() {
		defer w.globalWG.Done()

		tableIdentifier := fmt.Sprintf("%s.%s", cfg.GlobalCfg.SchemaName, cfg.GlobalCfg.TableName)
		streamPrepared := w.sCfg.StreamPrepared
		interval := w.sCfg.Interval

		// TODO according tag num ... to decide the stream_prepared, interval etc.
		if streamPrepared < 0 {
			streamPrepared = 2
		}
		if interval < 0 {
			interval = 250
		}

		fs := Flags{
			"--source":          "stdin",
			"--db-database":     cfg.DB.Database,
			"--db-master-host":  cfg.DB.MasterHost,
			"--db-master-port":  cfg.DB.MasterPort,
			"--db-user":         cfg.DB.User,
			"--time-format":     "raw",
			"--format":          "csv",
			"--delimiter":       util.DELIMITER,
			"--target":          tableIdentifier,
			"--interval":        interval,
			"--stream-prepared": streamPrepared,
		}

		var cmd *exec.Cmd
		cmd, w.stdin, w.stdout, w.stderr, err = util.StartMxgateStdin(w.sCfg.mxgatePath, fs.ToStr())
		if err != nil {
			startWG.Done()
			return
		}

		defer func() {
			w.stat.stopAt = time.Now()
			// Notify stdin writer finished
			close(w.finCh)
		}()

		for {
			endLoop := false

			select {
			case <-w.ctx.Done():
				return
			default:
				time.Sleep(time.Second)
				if strings.Contains(w.stdout.String(), "prepared insert") {
					w.stat.startAt = time.Now()
					startWG.Done()
					endLoop = true
				} else if len(w.stderr.String()) > 0 {
					err = mxerror.CommonError(strings.TrimSuffix(w.stderr.String(), "exit status 1\n"))
					startWG.Done()
					return
				}
			}

			if endLoop {
				break
			}
		}

		for {
			select {
			case <-w.ctx.Done():
				_ = cmd.Wait()
				return
			default:
				time.Sleep(time.Second)
				if strings.Contains(w.stdout.String(), "reach EOF") {
					return
				}
			}
		}
	}()
	startWG.Wait()
	return w.finCh, err
}

func (w *Writer) Stop() error {
	if w.stdin != nil {
		w.stdin.Close()
	}
	if w.stdout != nil {
		w.stdout.Reset()
	}
	if w.stderr != nil {
		w.stderr.Reset()
	}
	w.cancelFunc()
	w.globalWG.Wait()
	return nil
}

func (w *Writer) Write(m []byte, cnt, size int64) error {
	sizeToGate, err := w.stdin.Write(m)
	if err != nil {
		return err
	}
	w.stat.size += size
	w.stat.sizeToGate += int64(sizeToGate)
	w.stat.count += cnt
	return nil
}

func (w *Writer) GetStat() engine.Stat {
	return w.stat
}

func (w *Writer) WriteEOF() error {
	if w.stdin != nil {
		w.stdin.Close()
		w.stdin = nil
	}
	return nil
}

func (w *Writer) CreatePluginConfig() interface{} {
	return &Config{}
}

func (w *Writer) GetDefaultFlags() (*pflag.FlagSet, interface{}) {
	sCfg := &Config{}
	p := pflag.NewFlagSet("writer.stdin", pflag.ContinueOnError)
	// hidden configs for tuning mxgate
	p.IntVar(&sCfg.StreamPrepared, "writer-stream-prepared", -1, "stream-prepared for mxgate")
	p.IntVar(&sCfg.Interval, "writer-interval", -1, "interval for mxgate")
	p.StringVar(&sCfg.mxgatePath, "writer-mxgate-path", "", "path of mxgate")

	p.StringVar(&sCfg.ProgressFormat, "writer-progress-format", "list", "progress format, support \"list\", \"json\"")
	p.BoolVar(&sCfg.ProgressIncludeTableSize, "writer-progress-include-table-size", false, "whether progress include table size")
	p.BoolVar(&sCfg.ProgressWithTimezone, "writer-progress-with-timezone", false, "whether print time with timezone")
	_ = p.MarkHidden("writer-stream-prepared")
	_ = p.MarkHidden("writer-interval")
	_ = p.MarkHidden("writer-mxgate-path")
	return p, sCfg
}

func (w *Writer) IsNil() bool {
	return w == nil
}
