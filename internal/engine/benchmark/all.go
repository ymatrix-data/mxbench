package benchmark

import (
	"errors"
	"fmt"

	"github.com/spf13/pflag"
	"github.com/spf13/viper"

	"github.com/ymatrix-data/mxbench/internal/engine"
	ni "github.com/ymatrix-data/mxbench/internal/engine/benchmark/nil"
	"github.com/ymatrix-data/mxbench/internal/engine/benchmark/telematics"
	"github.com/ymatrix-data/mxbench/internal/util/log"
)

func GetBenchmark(cfg engine.BenchmarkConfig) func(engine.BenchmarkConfig) engine.IBenchmark {
	switch cfg.Plugin {
	case "telematics":
		return telematics.NewBenchmark
	case "nil":
		return ni.NewBenchmark
	}

	log.Fatal(errors.New(""), fmt.Sprintf("Unknown benchmark plugin %s", cfg.Plugin))

	return nil
}

func GetDefaultFlags(cfg engine.BenchmarkConfig) (*pflag.FlagSet, interface{}) {
	var pluginBenchmark engine.IBenchmark
	switch cfg.Plugin {
	case "telematics":
		pluginBenchmark = new(telematics.Benchmark)
	case "nil":
		pluginBenchmark = new(ni.Benchmark)
	}
	if pluginBenchmark != nil {
		return pluginBenchmark.GetDefaultFlags()
	}
	return nil, nil
}

func RenderPluginConfig(cfgViper *viper.Viper, cfg engine.BenchmarkConfig, renew bool, opts ...viper.DecoderConfigOption) (interface{}, error) {
	var result interface{} = cfg.PluginConfig

	if renew || result == nil {
		switch cfg.Plugin {
		case "telematics":
			result = new(telematics.Benchmark).CreatePluginConfig()
		case "nil":
			result = new(ni.Benchmark).CreatePluginConfig()
		}
	}

	if result == nil {
		return nil, nil
	}

	p, _ := GetDefaultFlags(cfg)
	if p != nil {
		v := viper.New()
		if err := v.BindPFlags(p); err != nil {
			return nil, fmt.Errorf("warning Benchmark plugin config [%s], %s", cfg.Plugin, err.Error())
		}
		if err := v.Unmarshal(&result, opts...); err != nil {
			return nil, fmt.Errorf("warning Benchmark plugin config [%s], %s", cfg.Plugin, err.Error())
		}
	}

	if cfgViper == nil {
		return result, nil
	}

	pluginKey := fmt.Sprintf("benchmark.%s", cfg.Plugin)
	if err := cfgViper.UnmarshalKey(pluginKey, &result, opts...); err != nil {
		return nil, fmt.Errorf("error unmarshal benchmark config: %s", err)
	}

	return result, nil
}
