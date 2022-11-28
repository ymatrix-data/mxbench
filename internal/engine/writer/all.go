package writer

import (
	"errors"
	"fmt"

	"github.com/spf13/pflag"
	"github.com/spf13/viper"

	"github.com/ymatrix-data/mxbench/internal/engine"
	"github.com/ymatrix-data/mxbench/internal/engine/writer/http"
	ni "github.com/ymatrix-data/mxbench/internal/engine/writer/nil"
	"github.com/ymatrix-data/mxbench/internal/engine/writer/stdin"
	"github.com/ymatrix-data/mxbench/internal/util/log"
)

func GetWriter(cfg engine.WriterConfig) func(engine.WriterConfig) engine.IWriter {
	switch cfg.Plugin {
	case "http":
		return http.NewWriter
	case "stdin":
		return stdin.NewWriter
	case "nil":
		return ni.NewWriter
	}

	log.Fatal(errors.New(""), fmt.Sprintf("Unknown writer plugin %s", cfg.Plugin))

	return nil

}

func GetDefaultFlags(cfg engine.WriterConfig) (*pflag.FlagSet, interface{}) {
	var pluginWriter engine.IWriter
	switch cfg.Plugin {
	case "http":
		pluginWriter = new(http.Writer)
	case "stdin":
		pluginWriter = new(stdin.Writer)
	case "nil":
		pluginWriter = new(ni.Writer)
	}
	if pluginWriter != nil {
		return pluginWriter.GetDefaultFlags()
	}
	return nil, nil
}

func RenderPluginConfig(cfgViper *viper.Viper, cfg engine.WriterConfig, renew bool, opts ...viper.DecoderConfigOption) (interface{}, error) {
	var result interface{} = cfg.PluginConfig

	if renew || result == nil {
		switch cfg.Plugin {
		case "http":
			result = new(http.Writer).CreatePluginConfig()
		case "stdin":
			result = new(stdin.Writer).CreatePluginConfig()
		case "nil":
			result = new(ni.Writer).CreatePluginConfig()
		}
	}

	if result == nil {
		return nil, nil
	}

	p, _ := GetDefaultFlags(cfg)
	if p != nil {
		v := viper.New()
		if err := v.BindPFlags(p); err != nil {
			return nil, fmt.Errorf("warning writer plugin config [%s], %s", cfg.Plugin, err.Error())
		}
		if err := v.Unmarshal(&result, opts...); err != nil {
			return nil, fmt.Errorf("warning writer plugin config [%s], %s", cfg.Plugin, err.Error())
		}
	}

	if cfgViper == nil {
		return result, nil
	}

	pluginKey := fmt.Sprintf("writer.%s", cfg.Plugin)
	if err := cfgViper.UnmarshalKey(pluginKey, &result, opts...); err != nil {
		return nil, fmt.Errorf("error unmarshal writer config: %s", err)
	}

	return result, nil
}
