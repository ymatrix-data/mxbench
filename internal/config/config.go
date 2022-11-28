package config

import (
	"fmt"
	"os"

	"github.com/spf13/pflag"

	"github.com/ymatrix-data/mxbench/internal/config/parser"
	"github.com/ymatrix-data/mxbench/internal/engine"
	"github.com/ymatrix-data/mxbench/internal/util/mxerror"
)

func Init() *engine.Config {
	pflag.CommandLine.SetOutput(os.Stdout)

	cfg, err := engine.NewConfig()
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		mxerror.FromError(err).OSExit()
	}

	p := parser.New(cfg)
	p.Parse()

	return cfg
}

func DoAfterInit(cfg *engine.Config) error {
	// validation
	err := cfg.GlobalCfg.DoAfterInit()
	if err != nil {
		return err
	}

	// derivation
	cfg.GeneratorCfg.GlobalConfig = &cfg.GlobalCfg
	return nil
}
