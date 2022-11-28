package generator

import (
	"errors"
	"fmt"

	"github.com/spf13/pflag"
	"github.com/spf13/viper"

	"github.com/ymatrix-data/mxbench/internal/engine"
	"github.com/ymatrix-data/mxbench/internal/engine/generator/file"
	ni "github.com/ymatrix-data/mxbench/internal/engine/generator/nil"
	"github.com/ymatrix-data/mxbench/internal/engine/generator/telematics"
	"github.com/ymatrix-data/mxbench/internal/util/log"
)

func GetGenerator(cfg engine.GeneratorConfig) func(engine.GeneratorConfig) engine.IGenerator {
	switch cfg.Plugin {
	case "telematics":
		return telematics.NewGenerator
	case "file":
		return file.NewGenerator
	case "nil":
		return ni.NewGenerator
	}

	log.Fatal(errors.New(""), fmt.Sprintf("Unknown generator plugin %s", cfg.Plugin))

	return nil
}

func GetDefaultFlags(cfg engine.GeneratorConfig) (*pflag.FlagSet, interface{}) {
	var pluginGenerator engine.IGenerator
	switch cfg.Plugin {
	case "telematics":
		pluginGenerator = new(telematics.Generator)
	case "file":
		pluginGenerator = new(file.Generator)
	case "nil":
		pluginGenerator = new(ni.Generator)
	}
	if pluginGenerator != nil {
		return pluginGenerator.GetDefaultFlags()
	}
	return nil, nil
}

func RenderPluginConfig(cfgViper *viper.Viper, cfg engine.GeneratorConfig, renew bool, opts ...viper.DecoderConfigOption) (interface{}, error) {
	var result interface{} = cfg.PluginConfig

	if renew || result == nil {
		switch cfg.Plugin {
		case "telematics":
			result = new(telematics.Generator).CreatePluginConfig()
		case "file":
			result = new(file.Generator).CreatePluginConfig()
		case "nil":
			result = new(ni.Generator).CreatePluginConfig()
		}
	}

	if result == nil {
		return nil, nil
	}

	p, _ := GetDefaultFlags(cfg)
	if p != nil {
		v := viper.New()
		if err := v.BindPFlags(p); err != nil {
			return nil, fmt.Errorf("warning Generator plugin config [%s], %s", cfg.Plugin, err.Error())
		}
		if err := v.Unmarshal(&result, opts...); err != nil {
			return nil, fmt.Errorf("warning Generator plugin config [%s], %s", cfg.Plugin, err.Error())
		}
	}

	if cfgViper == nil {
		return result, nil
	}

	pluginKey := fmt.Sprintf("generator.%s", cfg.Plugin)
	if err := cfgViper.UnmarshalKey(pluginKey, &result, opts...); err != nil {
		return nil, fmt.Errorf("error unmarshal generator config: %s", err)
	}

	return result, nil
}
