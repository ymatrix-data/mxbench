package parser

import (
	"errors"
	"fmt"
	"reflect"

	"github.com/mitchellh/mapstructure"
	"github.com/spf13/viper"
)

/**
* Parse toml file into configuration struct.
* If the value comes from cli, use the cli value,
* Otherwise, override it.
 */
type FileParser struct {
	*CmdParser
}

func newFileParser(cmdParser *CmdParser) *FileParser {
	return &FileParser{
		CmdParser: cmdParser,
	}
}

// Import config from given toml file
func (parser *FileParser) parse() (err error) {
	if !parser.isCmdParsed {
		panic(errors.New("need cmd parsed"))
	}

	cfg := parser.cfg

	if len(cfg.GlobalCfg.CfgFile) <= 0 {
		return nil
	}

	v := viper.New()
	v.SetConfigFile(cfg.GlobalCfg.CfgFile)
	v.SetConfigType("toml")

	err = v.ReadInConfig()
	if err != nil { // Handle errors reading the config file
		return fmt.Errorf("error reading config file: %s", err)
	}

	err = v.Unmarshal(cfg, func(c *mapstructure.DecoderConfig) {
		c.DecodeHook = parser.getDecodeHook(c)
	})
	if err != nil { // Handle errors reading the config file
		return fmt.Errorf("error parsing config file: %s", err)
	}

	err = parser.initPlugin(v, func(c *mapstructure.DecoderConfig) {
		c.DecodeHook = parser.getDecodeHook(c)
	})
	if err != nil {
		return err
	}

	return nil
}

func (parser *FileParser) getDecodeHook(c *mapstructure.DecoderConfig) mapstructure.DecodeHookFuncValue {
	return func(from reflect.Value, to reflect.Value) (interface{}, error) {
		tag2ValueMap, ok := from.Interface().(map[string]interface{})
		if !ok {
			return from.Interface(), nil
		}
		parser.delCliTag(tag2ValueMap)
		return from.Interface(), nil
	}
}

// If the value's source is cli,
// delete the key from tag2ValueMap,
// and the value would not be overrided by current config file.
// other case, the value would be overrided.
func (parser *FileParser) delCliTag(tag2ValueMap map[string]interface{}) {
	for tag, value := range tag2ValueMap {
		flag := parser.findFlag(tag)
		if flag == nil {
			innerTag2ValueMap, ok := value.(map[string]interface{})
			if ok {
				parser.delCliTag(innerTag2ValueMap)
				continue
			}
			continue
		}
		if !flag.Changed {
			continue
		}
		if parser.sourceEnvTag[tag] {
			continue
		}
		if parser.protectedKey[tag] {
			continue
		}
		delete(tag2ValueMap, tag)
	}
}
