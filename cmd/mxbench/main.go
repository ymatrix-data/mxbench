package main

import (
	"errors"
	"fmt"
	"math"
	"os"
	"os/signal"
	"path/filepath"
	"runtime"
	"runtime/pprof"
	"sync"
	"syscall"
	"time"

	"github.com/ymatrix-data/mxbench/internal/config"
	"github.com/ymatrix-data/mxbench/internal/engine"
	"github.com/ymatrix-data/mxbench/internal/engine/benchmark"
	"github.com/ymatrix-data/mxbench/internal/engine/generator"
	"github.com/ymatrix-data/mxbench/internal/engine/writer"
	"github.com/ymatrix-data/mxbench/internal/util"
	"github.com/ymatrix-data/mxbench/internal/util/log"
	"github.com/ymatrix-data/mxbench/internal/util/mxerror"
	"github.com/ymatrix-data/mxbench/testutils/injector"
)

var (
	gracefulQuitOnce sync.Once
	failQuitOnce     sync.Once
)

func main() {
	var cfg *engine.Config
	var err error
	var inExit int
	var lastExitSignalTime time.Time

	cfg = config.Init()
	cfg.NewGeneratorFunc = generator.GetGenerator(cfg.GeneratorCfg)
	cfg.NewWriterFunc = writer.GetWriter(cfg.WriterCfg)
	cfg.NewBenchmarkFunc = benchmark.GetBenchmark(cfg.BenchmarkCfg)

	err = config.DoAfterInit(cfg)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		mxerror.FromError(err).OSExit()
	}

	injector.PreEngineRun(cfg)

	err = log.InitLogger(cfg.GlobalCfg.LogLevel, "mxbench")
	if err != nil {
		_, _ = fmt.Fprintln(os.Stderr, err)
		mxerror.FromError(err).OSExit()
	}

	util.PrintLogo("https://www.ymatrix.cn")

	// Limit CPU used for mxbench, to avoid CPI hogging.
	nCpu := runtime.NumCPU()
	set := runtime.GOMAXPROCS(0)
	if nCpu == set {
		target := int(math.Sqrt(float64(nCpu)))
		if target < 1 {
			target = 1
		}
		if target > nCpu {
			target = nCpu
		}
		runtime.GOMAXPROCS(target)
		log.Info("Limit GOMAXPROCS to %d", target)
	} else {
		log.Info("GOMAXPROCS is %d", set)
	}

	if cfg.GlobalCfg.CPUProfile {
		err = startCPUProfile()
		if err != nil {
			panic(err)
		}
	}

	e, err := engine.NewEngineFromConfig(cfg)
	if err != nil {
		failQuit(e, err)
	}

	go func() {
		err = e.Run()
		if err != nil {
			failQuit(e, err)
		}
		gracefulQuit(e.Config, e)
	}()

	signalChan := make(chan os.Signal, 4)
	// Relay incoming signals
	signal.Notify(signalChan,
		syscall.SIGHUP,
		syscall.SIGINT,
		syscall.SIGTERM,
		syscall.SIGQUIT,
	)

	injector.PostEngineRun(cfg)

	for {
		s := <-signalChan
		log.Info("Got OS signal %v", s)
		switch s {
		case syscall.SIGHUP:
			// SIGHUP is noop
		case syscall.SIGINT, syscall.SIGTERM, syscall.SIGQUIT:
			// SIGINT/SIGTERM/SIGQUIT is quit signal
			// It triggers graceful quit once called, such as Ctrl+C or kill
			// If quit signal comes three times within a short period, it triggers
			// immediate quit, it will not wait generator/writer/benchmark to quit
			// TODO: it may lead to (but not limited to) following consequences:
			//  - mxgate not gracefully exited
			if time.Since(lastExitSignalTime) > time.Second*5 {
				// If last exit signal is 5s ago, reset the force quit counter
				inExit = 0
			}
			inExit++
			lastExitSignalTime = time.Now()
			switch inExit {
			case 1:
				// 1st quit signal
				go gracefulQuit(cfg, e)
			case 2:
				// 2nd quit signal
				log.Info("Exit in progress, signal again to force quit")
			default:
				// 3rd or later
				// output empty error msg
				log.Fatal(errors.New(""), "Force quit")
			}
		}
	}
}

func failQuit(e engine.IEngine, err error) {
	failQuitOnce.Do(func() {
		if !e.IsNil() {
			e.Close()
			injector.PostEngineClose()
		}
		mxerr, ok := err.(*mxerror.MxbenchError)
		if !ok {
			if !e.IsNil() {
				e.PrintStat()
				e.GetFormattedSummary()
			}

			mxerror.FromError(err).OSExit()
		}
		mxerr.OSExit()
	})
}

func gracefulQuit(cfg *engine.Config, e engine.IEngine) {
	// Perform a graceful stop
	gracefulQuitOnce.Do(func() {
		if !e.IsNil() {
			e.Close()
			injector.PostEngineClose()
			e.PrintStat()
			e.GetFormattedSummary()
		}
		if cfg.GlobalCfg.CPUProfile {
			stopCPUProfile()
		}
		log.Info("mxbench exit normally")
		os.Exit(0)
	})
}

func startCPUProfile() (err error) {
	var fh *os.File

	file := filepath.Join("/tmp", fmt.Sprintf("cpuprofile.%s.dat", time.Now().Format("2006-01-02_150405")))
	log.Info("CPU profile at %s", file)

	if fh, err = os.Create(file); err == nil {
		err = pprof.StartCPUProfile(fh)
	}

	return
}

func stopCPUProfile() {
	pprof.StopCPUProfile()
}
