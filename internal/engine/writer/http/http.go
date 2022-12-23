package http

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"os/exec"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/spf13/pflag"
	"github.com/valyala/fasthttp"

	"github.com/ymatrix-data/mxbench/internal/engine"
	"github.com/ymatrix-data/mxbench/internal/util"
	"github.com/ymatrix-data/mxbench/internal/util/log"
	"github.com/ymatrix-data/mxbench/internal/util/mxerror"
)

const (
	_HEADER_CONTENT_ENCODING = "Content-Encoding"
	_HEADER_GZIP             = "gzip"
	_BATCH_SIZE              = 4 * 1024 * 1024
	_BATCH_RED               = _BATCH_SIZE / 8 * 7
	_METHOD_POST             = "POST"
	_TEXT_PLAIN              = "text/plain"
	_HTTP_PORT               = 8086
)

var (
	errBackoff          = fmt.Errorf("backpressure is needed")
	backoffMagicWords0  = []byte("engine: cache maximum memory size exceeded")
	backoffMagicWords1  = []byte("write failed: hinted handoff queue not empty")
	backoffMagicWords2a = []byte("write failed: read message type: read tcp")
	backoffMagicWords2b = []byte("i/o timeout")
	backoffMagicWords3  = []byte("write failed: engine: cache-max-memory-size exceeded")
	backoffMagicWords4  = []byte("timeout")
	backoffMagicWords5  = []byte("write failed: can not exceed max connections of 500")
)

type Flags map[string]interface{}

func (fs Flags) ToStr() string {
	var vars []string
	for k, v := range fs {
		vars = append(vars, fmt.Sprintf("%s=%v", k, v))
	}
	return strings.Join(vars, " ")
}

type Config struct {
	Parallel int  `mapstructure:"writer-parallel"`
	UseGzip  bool `mapstructure:"writer-use-gzip"`

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

type Writer struct {
	hCfg *Config

	ctx        context.Context
	cancelFunc context.CancelFunc
	finCh      chan error

	stat *Stat

	gateOut io.Reader

	url      string
	batchCh  chan *sendAndFeed
	globalWG sync.WaitGroup

	tableName string
}

func NewWriter(cfg engine.WriterConfig) engine.IWriter {
	hCfg := cfg.PluginConfig.(*Config)
	ctx, cancelFunc := context.WithCancel(context.Background())
	return &Writer{
		hCfg:       hCfg,
		ctx:        ctx,
		cancelFunc: cancelFunc,
		finCh:      make(chan error, hCfg.Parallel),
		batchCh:    make(chan *sendAndFeed, 100),
		url:        fmt.Sprintf("http://127.0.0.1:%d", _HTTP_PORT),
		stat:       &Stat{},
	}
}

func newStat(volumeDesc engine.VolumeDesc, cfg *Config) *Stat {
	return &Stat{volumeDesc: volumeDesc, config: cfg}
}

func (w *Writer) Start(cfg engine.Config, volumeDesc engine.VolumeDesc) (<-chan error, error) {
	w.stat = newStat(volumeDesc, w.hCfg)
	w.tableName = fmt.Sprintf("%s.%s", cfg.GlobalCfg.SchemaName, cfg.GlobalCfg.TableName)

	var err error
	var startWG sync.WaitGroup

	startWG.Add(1)
	w.globalWG.Add(1)

	go func() {
		// TODO
		defer w.globalWG.Done()

		// TODO according tag num ... to decide the stream_prepared
		tableIdentifier := fmt.Sprintf("%s.%s", cfg.GlobalCfg.SchemaName, cfg.GlobalCfg.TableName)
		streamPrepared := w.hCfg.StreamPrepared
		interval := w.hCfg.Interval

		// TODO according tag num ... to decide the stream_prepared, interval etc.
		if streamPrepared < 0 {
			streamPrepared = 2
		}
		if interval < 0 {
			interval = 250
		}
		useGzip := "no"
		if w.hCfg.UseGzip {
			useGzip = "yes"
		}
		fs := Flags{
			"--source":          "http",
			"--format":          "csv",
			"--time-format":     "raw",
			"--http-port":       _HTTP_PORT,
			"--max-body-bytes":  _BATCH_SIZE * 2, // Avoid batchSize bigger than --max-body-bytes
			"--interval":        interval,
			"--stream-prepared": streamPrepared,
			"--use-gzip":        useGzip,

			"--db-database":    cfg.DB.Database,
			"--db-master-host": cfg.DB.MasterHost,
			"--db-master-port": cfg.DB.MasterPort,
			"--db-user":        cfg.DB.User,
			"--delimiter":      util.DELIMITER,
			"--target":         tableIdentifier,

			"--timing":                  "true",
			"--metrics-sample-interval": 15,

			// To benchmark generate speed
			// "--writer":    "nil",
			// "--transform": "nil",
		}

		var cmd *exec.Cmd
		cmd, w.gateOut, err = util.StartMxgate(w.hCfg.mxgatePath, fs.ToStr())
		// fmt.Println("mxgate:", cmd.String())
		if err != nil {
			startWG.Done()
			return
		}

		defer func() {
			w.stat.stopAt = time.Now()
			// Notify http writer finished
			close(w.finCh)
		}()

		func() {
			var out string
			var n int
			b := make([]byte, 1024)
			defer func() {
				startWG.Done()
				go func() {
					// consume gate stdout to prevent hang
					_, _ = io.ReadAll(w.gateOut)
				}()
			}()
			for {
				select {
				case <-w.ctx.Done():
					return
				default:
					time.Sleep(time.Second)
					for {
						n, err = w.gateOut.Read(b)
						if n > 0 {
							out += string(b)
						}
						if err == io.EOF {
							err = mxerror.CommonError(out)
							return
						} else if err != nil {
							log.Error("read error %s", err)
							return
						}
						if n < len(b) {
							break
						}
					}

					if strings.Contains(out, "http listening on") {
						w.stat.startAt = time.Now()
						return
					} else if strings.Contains(out, "exit status") {
						err = mxerror.CommonError(out)
						return
					}
				}
			}
		}()

		// Send data to mxgate until completed
		w.send()
		_ = cmd.Process.Signal(syscall.SIGQUIT)
		_ = cmd.Wait()
	}()
	startWG.Wait()
	return w.finCh, err
}

var gAccPost, gNPost, gAccSize, gMaxSize, gMaxPostTime int64
var mu sync.Mutex

func (w *Writer) send() {
	var wg sync.WaitGroup

	wg.Add(w.hCfg.Parallel)

	for i := 0; i < w.hCfg.Parallel; i++ {
		go func(idx int) {
			var accPost, maxPostTime, accPostSize, maxPostSize int64
			var nPost int
			var batchBuf = bytes.NewBuffer(make([]byte, 0, _BATCH_SIZE))

			c := &fasthttp.HostClient{
				Addr: fmt.Sprintf("127.0.0.1:%d", _HTTP_PORT),
			}

			defer func() {
				c.CloseIdleConnections()
				mu.Lock()
				gAccPost += accPost
				gNPost += int64(nPost)
				gAccSize += accPostSize
				if maxPostSize > gMaxSize {
					gMaxSize = maxPostSize
				}
				if maxPostTime > gMaxPostTime {
					gMaxPostTime = maxPostTime
				}
				mu.Unlock()
				wg.Done()
			}()

			for {
				select {
				case <-w.ctx.Done():
					return

				case body, ok := <-w.batchCh:
					if !ok {
						if batchBuf.Len() > 0 {
							nPost++
							size := int64(batchBuf.Len())
							accPostSize += size
							if size > maxPostSize {
								maxPostSize = size
							}
							_, dur, err := w.post(c, batchBuf.Bytes())
							if dur > maxPostTime {
								maxPostTime = dur
							}
							accPost += dur
							if err != nil {
								fmt.Printf("err occurs 3 %v\n", err)
								w.finCh <- err
							}
						}
						return
					}

					if batchBuf.Len() <= 0 {
						_, err := batchBuf.WriteString(w.tableName + "\n")
						if err != nil {
							fmt.Printf("err occurs 1 %v\n", err)
							close(body.feed)
							return
						}
					}

					_, err := batchBuf.Write(body.msg)
					close(body.feed)
					if err != nil {
						fmt.Printf("err occurs 2 %v\n", err)
						return
					}

					if batchBuf.Len() >= _BATCH_RED {
						nPost++
						size := int64(batchBuf.Len())
						accPostSize += size
						if size > maxPostSize {
							maxPostSize = size
						}
						_, dur, err := w.post(c, batchBuf.Bytes())
						if dur > maxPostTime {
							maxPostTime = dur
						}
						accPost += dur
						if err != nil {
							fmt.Printf("err occurs %v\n", err)
							w.finCh <- err
							return
						}
						batchBuf.Reset()
					}
				}
			}
		}(i)
	}
	wg.Wait()
}

func (w *Writer) Stop() error {
	w.cancelFunc()
	w.globalWG.Wait()

	log.Verbose("Parallel: %d", w.hCfg.Parallel)
	log.Verbose("Acc Post: %s", time.Duration(gAccPost))
	log.Verbose("Num Post: %d", gNPost)
	log.Verbose("Slowest Post: %s", time.Duration(gMaxPostTime))
	log.Verbose("Acc Post Size: %d", gAccSize)
	log.Verbose("Max Post Size: %d", gMaxSize)

	return nil
}

type sendAndFeed struct {
	msg  []byte
	feed chan struct{}
}

func (w *Writer) Write(msg []byte, msgCnt, msgSize int64) error {
	atomic.AddInt64(&w.stat.size, msgSize)
	atomic.AddInt64(&w.stat.count, msgCnt)
	atomic.AddInt64(&w.stat.sizeToGate, int64(len(msg)))

	ch := make(chan struct{})
	w.batchCh <- &sendAndFeed{
		msg:  msg,
		feed: ch,
	}
	<-ch
	return nil
}

func (w *Writer) WriteEOF() error {
	close(w.batchCh)
	return nil
}

func (w *Writer) GetStat() engine.Stat {
	return w.stat
}

func (w *Writer) CreatePluginConfig() interface{} {
	return &Config{}
}

func (w *Writer) GetDefaultFlags() (*pflag.FlagSet, interface{}) {
	hCfg := &Config{}
	p := pflag.NewFlagSet("writer.http", pflag.ContinueOnError)
	p.IntVar(&hCfg.Parallel, "writer-parallel", 8, "The parallel of http writer")
	p.BoolVar(&hCfg.UseGzip, "writer-use-gzip", false, "use gzip for http writer")

	// hidden configs for tuning mxgate
	p.IntVar(&hCfg.StreamPrepared, "writer-stream-prepared", -1, "stream-prepared for mxgate")
	p.IntVar(&hCfg.Interval, "writer-interval", -1, "interval for mxgate")
	p.StringVar(&hCfg.mxgatePath, "writer-mxgate-path", "", "path of mxgate")

	p.StringVar(&hCfg.ProgressFormat, "writer-progress-format", "list", "progress format. support \"list\", \"json\"")
	p.BoolVar(&hCfg.ProgressIncludeTableSize, "writer-progress-include-table-size", false, "whether progress include table size")
	p.BoolVar(&hCfg.ProgressIncludeTableSize, "writer-progress-with-timezone", false, "whether print time with timezone")

	_ = p.MarkHidden("writer-stream-prepared")
	_ = p.MarkHidden("writer-interval")
	return p, hCfg
}

func (w *Writer) post(c *fasthttp.HostClient, body []byte) (int, int64, error) {
	req := fasthttp.AcquireRequest()
	resp := fasthttp.AcquireResponse()
	defer func() {
		fasthttp.ReleaseRequest(req)
		fasthttp.ReleaseResponse(resp)
	}()

	req.SetRequestURI(w.url)
	req.Header.SetContentType(_TEXT_PLAIN)
	req.Header.SetMethod(_METHOD_POST)

	// TODO: mxbench send to mxgate don't do gzip, because it in same host.
	// if w.hCfg.UseGzip {
	// 	var b bytes.Buffer
	// 	gz := gzip.NewWriter(&b)
	// 	if _, err := gz.Write(body); err != nil {
	// 		return 0, 0, err
	// 	}
	// 	if err := gz.Close(); err != nil {
	// 		return 0, 0, err
	// 	}
	// 	body = b.Bytes()
	// 	req.Header.Add(_HEADER_CONTENT_ENCODING, _HEADER_GZIP)
	// }
	req.SetBody(body)

	ts := time.Now()
	err := c.Do(req, resp)
	if err != nil {
		// TODO response size
		return 0, 0, err
	}
	dur := time.Since(ts).Nanoseconds()

	sc := resp.StatusCode()
	switch sc {
	case fasthttp.StatusOK:
		if len(body) > 64 {
			err = fmt.Errorf("ERROR %d: %s: %s", sc, resp.Body(), string(body[:64]))
		} else {
			err = fmt.Errorf("ERROR %d: %s: %s", sc, resp.Body(), string(body))
		}
	case fasthttp.StatusNoContent:
		err = nil
	case fasthttp.StatusMethodNotAllowed:
		err = fmt.Errorf("ERROR bad request method")
	case fasthttp.StatusServiceUnavailable:
		err = fmt.Errorf("WARN mxgate exit")
	case fasthttp.StatusInternalServerError:
		if w.backpressurePred(resp.Body()) {
			err = errBackoff
		} else {
			if len(body) > 64 {
				err = fmt.Errorf("ERROR %d: %s: %s", sc, resp.Body(), string(body[:64]))
			} else {
				err = fmt.Errorf("ERROR %d: %s: %s", sc, resp.Body(), string(body))
			}
		}
	default:
		if len(body) > 64 {
			err = fmt.Errorf("ERROR %d: %s: %s", sc, resp.Body(), string(body[:64]))
		} else {
			err = fmt.Errorf("ERROR %d: %s: %s", sc, resp.Body(), string(body))
		}
	}

	return len(body), dur, err
}

func (w *Writer) backpressurePred(body []byte) bool {
	if bytes.Contains(body, backoffMagicWords0) {
		return true
	} else if bytes.Contains(body, backoffMagicWords1) {
		return true
	} else if bytes.Contains(body, backoffMagicWords2a) && bytes.Contains(body, backoffMagicWords2b) {
		return true
	} else if bytes.Contains(body, backoffMagicWords3) {
		return true
	} else if bytes.Contains(body, backoffMagicWords4) {
		return true
	} else if bytes.Contains(body, backoffMagicWords5) {
		return true
	} else {
		return false
	}
}

func (w *Writer) IsNil() bool {
	return w == nil
}
