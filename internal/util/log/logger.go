package log

import (
	"fmt"
	"log"
	"math"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/TwiN/go-color"
	"github.com/pkg/errors"
)

type Level int

const (
	logfatal Level = iota
	LOGERROR
	logwarn
	LOGINFO
	LOGVERBOSE
	LOGDEBUG
)

var mapper = map[Level]string{
	LOGERROR:   "ERROR",
	logfatal:   "CRITICAL",
	logwarn:    "WARN",
	LOGINFO:    "INFO",
	LOGVERBOSE: "VERBOSE",
	LOGDEBUG:   "DEBUG",
}

func (l Level) String() string {
	return mapper[l]
}

var (
	now   = time.Now
	logfd *mxLogger
)

type option struct{}

type AdaptorOption func(*option)

func NewLogger(opts ...Option) Logger {
	return &loggerCopy{
		logid: defaultLogIDGen()(),
		index: make(map[string]int),
	}
}

type (
	LogPrefixFunc func(string) string
	ExitFunc      func()
)

type mxLogger struct {
	Config

	stderr *log.Logger
	stdout *log.Logger
	file   *log.Logger

	header string

	mu sync.Mutex
}

func defaultLogIDGen() func() string {
	var (
		mu   sync.RWMutex
		id   uint16
		last int64
	)

	return func() string {
		mu.Lock()
		defer mu.Unlock()

		if last == 0 {
			last = now().UnixNano()
			return fmt.Sprintf("%d00000", last)
		}
		if id < math.MaxUint16 {
			id++
		} else {
			for {
				now := now().UnixNano()
				if now <= last {
					time.Sleep(time.Duration(last-now) * time.Nanosecond)
					continue
				}
				last = now
				id = 0
				break
			}
		}
		return fmt.Sprintf("%d%05d", last, id)
	}
}

type Logger interface {
	Verbose(string, ...interface{})
	Debug(string, ...interface{})
	Info(string, ...interface{})
	Warn(string, ...interface{})
	Error(string, ...interface{})
	Fatal(string, ...interface{})

	PutMetadata(string, string)
	Copy() Logger
}

type loggerCopy struct {
	logid string
	index map[string]int
	ks    []string
	vs    []string
	mu    sync.RWMutex
}

func (lc *loggerCopy) PutMetadata(k, v string) {
	lc.mu.Lock()
	defer lc.mu.Unlock()

	idx, ok := lc.index[k]
	if ok {
		lc.vs[idx] = v
	} else {
		lc.ks = append(lc.ks, k)
		lc.vs = append(lc.vs, v)
		lc.index[k] = len(lc.ks) - 1
	}
}

func (lc *loggerCopy) Copy() Logger {
	lc.mu.Lock()
	defer lc.mu.Unlock()

	if len(lc.index) == 0 {
		return &loggerCopy{
			logid: lc.logid,
			index: make(map[string]int),
		}
	}

	var (
		index = make(map[string]int, len(lc.ks))
		ks    = make([]string, len(lc.ks))
		vs    = make([]string, len(lc.ks))
	)
	for idx := range lc.ks {
		ks[idx] = lc.ks[idx]
		vs[idx] = lc.vs[idx]
		index[ks[idx]] = idx
	}

	return &loggerCopy{
		logid: lc.logid,
		index: index,
		ks:    ks,
		vs:    vs,
	}
}

func (lc *loggerCopy) baseInfo() string {
	lc.mu.RLock()
	defer lc.mu.RUnlock()

	tmp := make([]string, 0, 1+len(lc.ks))
	tmp = append(tmp, fmt.Sprintf("[logid:%s]", lc.logid))
	for idx := range lc.ks {
		tmp = append(tmp, fmt.Sprintf("[%s:%s]", lc.ks[idx], lc.vs[idx]))
	}
	return strings.Join(tmp, " ")
}

func (lc *loggerCopy) Verbose(format string, args ...interface{}) {
	format = fmt.Sprintf(format, args...)
	logfd.output(3, LOGDEBUG, fmt.Sprintf("%s %s", lc.baseInfo(), format))
}

func (lc *loggerCopy) Debug(format string, args ...interface{}) {
	format = fmt.Sprintf(format, args...)
	logfd.output(3, LOGVERBOSE, fmt.Sprintf("%s %s", lc.baseInfo(), format))
}

func (lc *loggerCopy) Info(format string, args ...interface{}) {
	format = fmt.Sprintf(format, args...)
	logfd.output(3, LOGINFO, fmt.Sprintf("%s %s", lc.baseInfo(), format))
}

func (lc *loggerCopy) Warn(format string, args ...interface{}) {
	format = fmt.Sprintf(format, args...)
	logfd.output(3, logwarn, fmt.Sprintf("%s %s", lc.baseInfo(), format))
}

func (lc *loggerCopy) Error(format string, args ...interface{}) {
	format = fmt.Sprintf(format, args...)
	logfd.output(3, LOGERROR, fmt.Sprintf("%s %s", lc.baseInfo(), format))
}

func (lc *loggerCopy) Fatal(format string, args ...interface{}) {
	format = fmt.Sprintf(format, args...)
	logfd.output(3, logfatal, fmt.Sprintf("%s %s", lc.baseInfo(), format))
}

func GetLogFilePath() string {
	return logfd.filename
}

func (logger *mxLogger) addColor(logVerbosity Level) string {
	switch logVerbosity {
	case logfatal:
		fallthrough
	case LOGERROR:
		return color.InRed(logVerbosity.String())
	case logwarn:
		return color.InYellow(logVerbosity.String())
	case LOGINFO:
		return color.InBlue(logVerbosity.String())
	case LOGVERBOSE:
		fallthrough
	case LOGDEBUG:
		return color.InGreen(logVerbosity.String())
	}
	return logVerbosity.String()
}

func (logger *mxLogger) output(depth int, logVerbosity Level, msg string) {
	logger.mu.Lock()
	defer logger.mu.Unlock()

	if len(logger.prefix) > 0 {
		msg = stringConnect(logger.prefix, msg)
	}

	rawMsg := stringConnect(logger.prefixFunc(logVerbosity.String()), msg)
	colorMsg := stringConnect(logger.prefixFunc(logger.addColor(logVerbosity)), msg)
	if !logger.color {
		colorMsg = rawMsg
	}

	if logger.onlyToStd && logVerbosity <= logger.shellVerbosity {
		_ = logger.stderr.Output(depth, colorMsg)
	} else {
		if logger.alsoToStd && logVerbosity <= logger.shellVerbosity {
			switch logVerbosity {
			case logfatal:
				fallthrough
			case LOGERROR:
				_ = logger.stderr.Output(depth, colorMsg)
			case logwarn:
				fallthrough
			case LOGINFO:
				fallthrough
			case LOGVERBOSE:
				fallthrough
			case LOGDEBUG:
				_ = logger.stdout.Output(depth, colorMsg)
			}
		}
		if logVerbosity <= logger.fileVerbosity {
			_= logger.file.Output(depth, rawMsg)
		}
	}
}

func Info(s string, v ...interface{}) {
	logfd.output(3, LOGINFO, fmt.Sprintf(s, v...))
}

func Warn(s string, v ...interface{}) {
	logfd.output(3, logwarn, fmt.Sprintf(s, v...))
}

func Verbose(s string, v ...interface{}) {
	logfd.output(3, LOGVERBOSE, fmt.Sprintf(s, v...))
}

func Debug(s string, v ...interface{}) {
	logfd.output(3, LOGDEBUG, fmt.Sprintf(s, v...))
}

func Error(s string, v ...interface{}) {
	logfd.output(3, LOGERROR, fmt.Sprintf(s, v...))
}

func Fatal(err error, s string, v ...interface{}) {
	fatalWithDepth(4, err, s, v...)
}

func fatalWithDepth(depth int, err error, s string, v ...interface{}) {
	var (
		critical      = mapper[logfatal]
		msg           = logfd.prefixFunc(critical)
		stackTraceStr string
	)
	if err == nil {
		err = fmt.Errorf("")
	} else {
		msg = stringConnect(msg, fmt.Sprintf("%v", err))
	}
	stackTraceStr = formatStackTrace(errors.WithStack(err))
	if s != "" && err.Error() != "" {
		msg = stringConnect(msg, ": ")
	}

	msg = stringConnect(msg, strings.TrimSpace(fmt.Sprintf(s, v...)))
	logfd.output(depth, logfatal, stringConnect(msg, stackTraceStr))

	os.Exit(1)
}

func FatalOnError(err error, output ...interface{}) {
	if err != nil {
		if len(output) == 0 {
			fatalWithDepth(4, err, "")
		} else if len(output) == 1 {
			fatalWithDepth(4, err, output[0].(string))
		} else {
			fatalWithDepth(4, err, output[0].(string), output[1:])
		}
	}
}

func stringConnect(elems ...string) string {
	if len(elems) <= 0 {
		return ""
	}
	var sb strings.Builder
	for _, elem := range elems {
		sb.WriteString(elem)
	}
	return sb.String()
}

type stackTracer interface {
	StackTrace() errors.StackTrace
}

func formatStackTrace(err error) string {
	st := err.(stackTracer).StackTrace()
	message := fmt.Sprintf("%+v", st[1:])
	return message
}
