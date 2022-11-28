package parser

import (
	"bufio"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/pkg/errors"
	"github.com/spf13/pflag"
	"github.com/spf13/viper"
	"golang.org/x/text/cases"
	"golang.org/x/text/language"

	"github.com/ymatrix-data/mxbench/internal/engine"
	"github.com/ymatrix-data/mxbench/internal/engine/benchmark"
	"github.com/ymatrix-data/mxbench/internal/engine/generator"
	"github.com/ymatrix-data/mxbench/internal/engine/writer"
	"github.com/ymatrix-data/mxbench/internal/util"
)

type CfgKey struct {
	set string
	key string
}

type CfgKeys []*CfgKey

func (keys CfgKeys) isItemIn(set string, key string) bool {
	for _, f := range keys {
		if f.set == set && f.key == key {
			return true
		}
	}
	return false
}

// Filter following items not to appear in export config file
var filterItems = CfgKeys{}

// Items must not be commented in config file, even using default value
var mandatoryItems = CfgKeys{
	{"generator", "generator"},
	{"benchmark", "benchmark"},
	{"writer", "writer"},
}

type MatrixFlagSet struct {
	Label  string
	FSet   *pflag.FlagSet
	SubSet MatrixFlagSets
}

type MatrixFlagSets []MatrixFlagSet

/**
 * Parse cli flags into configuration struct.
 * Print usage info
 * Print configuration struct into toml
 */
type FlagsParser struct {
	// Protect field value effect by cli flag
	protectedKey map[string]bool
	// Cache exited flag name
	existFlagMap map[string]bool
	FlagSets     MatrixFlagSets
	mainFlagSet  *pflag.FlagSet
	tempPath     string
	isFlagParsed bool

	RenderGeneratorConfigFunc func(*viper.Viper, engine.GeneratorConfig, bool, ...viper.DecoderConfigOption) (interface{}, error)
	RenderWriterConfigFunc    func(*viper.Viper, engine.WriterConfig, bool, ...viper.DecoderConfigOption) (interface{}, error)
	RenderBenchmarkConfigFunc func(*viper.Viper, engine.BenchmarkConfig, bool, ...viper.DecoderConfigOption) (interface{}, error)

	GetGeneratorDefaultFlagsFunc func(engine.GeneratorConfig) (*pflag.FlagSet, interface{})
	GetWriterDefaultFlagsFunc    func(engine.WriterConfig) (*pflag.FlagSet, interface{})
	GetBenchmarkDefaultFlagsFunc func(engine.BenchmarkConfig) (*pflag.FlagSet, interface{})

	// Target
	cfg *engine.Config
}

func newFlagsParser(cfg *engine.Config) *FlagsParser {
	parser := &FlagsParser{
		FlagSets:     MatrixFlagSets{},
		protectedKey: map[string]bool{},
		existFlagMap: map[string]bool{},
		cfg:          cfg,
	}

	parser.RenderGeneratorConfigFunc = generator.RenderPluginConfig
	parser.RenderWriterConfigFunc = writer.RenderPluginConfig
	parser.RenderBenchmarkConfigFunc = benchmark.RenderPluginConfig

	parser.GetGeneratorDefaultFlagsFunc = generator.GetDefaultFlags
	parser.GetWriterDefaultFlagsFunc = writer.GetDefaultFlags
	parser.GetBenchmarkDefaultFlagsFunc = benchmark.GetDefaultFlags

	main := pflag.NewFlagSet("main", pflag.ContinueOnError)
	main.SetOutput(os.Stdout)
	main.AddFlagSet(pflag.CommandLine)
	main.SortFlags = false
	main.Usage = parser.Usage
	parser.mainFlagSet = main

	cfg.Usage = parser.Usage

	tempDir := util.TempDir()
	parser.tempPath = filepath.Join(tempDir, fmt.Sprintf("mxbenchcfg.%d.toml", os.Getpid()))

	return parser
}

func (parser *FlagsParser) addFlagSet(label string, set *pflag.FlagSet, subSets ...MatrixFlagSet) {
	set.VisitAll(func(flag *pflag.Flag) {
		if parser.existFlagMap[flag.Name] {
			panic(fmt.Errorf("duplicate key: %s", flag.Name))
		}
		parser.existFlagMap[flag.Name] = true
	})
	parser.mainFlagSet.AddFlagSet(set)
	if label != "" {
		parser.FlagSets = append(parser.FlagSets, MatrixFlagSet{label, set, subSets})
	}
}

func (parser *FlagsParser) parse() error {
	defer func() { parser.isFlagParsed = true }()

	cfg := parser.cfg

	fSetGlobal := cfg.GlobalFlagSet()
	parser.addFlagSet("global", fSetGlobal)

	fSetDB := cfg.DBFlagSet()
	parser.addFlagSet("database", fSetDB)

	fSetGenerator, fSetGeneratorSub := parser.InitGeneratorFlagSet(&cfg.GeneratorCfg)
	parser.addFlagSet("generator", fSetGenerator, fSetGeneratorSub...)

	fSetBenchmark, fSetBenchmarkSub := parser.InitBenchmarkFlagSet(&cfg.BenchmarkCfg)
	parser.addFlagSet("benchmark", fSetBenchmark, fSetBenchmarkSub...)

	fSetWriter, fSetWriterSub := parser.InitWriterFlagSet(&cfg.WriterCfg)
	parser.addFlagSet("writer", fSetWriter, fSetWriterSub...)

	if err := parser.mainFlagSet.Parse(os.Args[1:]); err != nil {
		fmt.Println(err)
		return errParseFlags
	}

	if err := parser.initPlugin(nil); err != nil {
		return errParseFlags
	}

	return nil
}

// prints help info
func (parser *FlagsParser) Usage() {
	fmt.Println("Usage of mxbench")
	fmt.Printf("%s <command> [<args>]\n", util.CLI_BIN)
	fmt.Println("")
	fmt.Println("The commands are:")
	fmt.Println("    run            Run mxbench in command line")
	fmt.Println("    config         Print full sample configuration to STDOUT")
	fmt.Println("    help           Show usage")
	fmt.Println("    version        Show version")
	fmt.Println("")
	fmt.Println("The arguments are:")

	caser := cases.Title(language.Und)
	for _, flagSet := range parser.FlagSets {
		fmt.Printf("\n  %s Options:\n", caser.String(flagSet.Label))
		flagSet.FSet.PrintDefaults()
		for _, subFlagSet := range flagSet.SubSet {
			subFlagSet.FSet.PrintDefaults()
		}
	}

	fmt.Printf(`
Examples:

    # generate a mxbench config file with given args:
    %[1]s config [<args...>] > mxbench.conf

    # example to generate with listed argements, all non-listed values be default:
    %[1]s config --db-master-port 6000 > mxbench.conf

    # edit mxbench.conf with your customized configuration for each plugin:
    # such as delimiter or time format
    vim mxbench.conf

    # launch mxbench with the config file:
    %[1]s --config mxbench.conf

    # launch mxbench with the config file, override a handful of args:
    %[1]s run --config mxbench.conf --db-master-port 7000

    # launch mxbench without a config file:
    %[1]s run [<args...>]
`, util.CLI_BIN)
}

func (parser *FlagsParser) initPlugin(v *viper.Viper, opts ...viper.DecoderConfigOption) (err error) {
	cfg := parser.cfg
	if cfg.GeneratorCfg.Plugin != "" {
		if v != nil {
			cliGenerator := parser.findFlag("generator")
			cfgGenerator := v.GetString("generator.generator")
			renew := false
			if cliGenerator.Changed {
				if cliGenerator.Value.String() != cfgGenerator {
					return fmt.Errorf("conflict generator configuration, cli: %s, config: %s", cliGenerator.Value.String(), cfgGenerator)
				}
			} else {
				if cfgGenerator != "telematics" && cfgGenerator != "" {
					renew = true
				}
			}
			cfg.GeneratorCfg.PluginConfig, err = parser.RenderGeneratorConfigFunc(v, cfg.GeneratorCfg, renew, opts...)
			if err != nil {
				return
			}
		}
	}
	if cfg.BenchmarkCfg.Plugin != "" {
		if v != nil {
			cliBench := parser.findFlag("benchmark")
			cfgBench := v.GetString("benchmark.benchmark")
			renew := false
			if cliBench.Changed {
				if cliBench.Value.String() != cfgBench {
					return fmt.Errorf("conflict benchmark configuration, cli: %s, config: %s", cliBench.Value.String(), cfgBench)
				}
			} else {
				if cfgBench != "telematics" && cfgBench != "" {
					renew = true
				}
			}
			cfg.BenchmarkCfg.PluginConfig, err = parser.RenderBenchmarkConfigFunc(v, cfg.BenchmarkCfg, renew, opts...)
			if err != nil {
				return
			}
		}
	}
	if cfg.WriterCfg.Plugin != "" {
		if v != nil {
			cliWriter := parser.findFlag("writer")
			cfgWriter := v.GetString("writer.writer")
			renew := false
			if cliWriter.Changed {
				if cliWriter.Value.String() != cfgWriter {
					return fmt.Errorf("conflict writer configuration, cli: %s, config: %s", cliWriter.Value.String(), cfgWriter)
				}
			} else {
				if cfgWriter != "http" && cfgWriter != "" {
					renew = true
				}
			}
			cfg.WriterCfg.PluginConfig, err = parser.RenderWriterConfigFunc(v, cfg.WriterCfg, renew, opts...)
			if err != nil {
				return
			}
		}
	}
	return
}

// Print current command line flags in toml format
func (parser *FlagsParser) Print() error {
	err := parser.writeToTemp()
	if err != nil {
		return errors.Errorf("[Config] failed: %s", err)
	}
	err = parser.beautifyPrintTemp()
	if err != nil {
		return errors.Errorf("[Config] failed: %s", err)
	}
	return nil
}

// Write current config to temp file
func (parser *FlagsParser) writeToTemp() error {
	for _, fs := range parser.FlagSets {
		v := viper.New()
		var subSet string

		err := v.BindPFlags(fs.FSet)
		if err != nil {
			return err
		}

		switch fs.Label {
		case "generator":
			subSet = parser.cfg.GeneratorCfg.Plugin
		case "writer":
			subSet = parser.cfg.WriterCfg.Plugin
		case "benchmark":
			subSet = parser.cfg.BenchmarkCfg.Plugin
		}

		if len(fs.SubSet) > 0 {
			sv := viper.New()
			for _, ss := range fs.SubSet {
				err = sv.BindPFlags(ss.FSet)
				if err != nil {
					return fmt.Errorf("bind flags: %s", err)
				}
			}
			v.Set(subSet, sv.AllSettings())
		}

		viper.Set(fs.Label, v.AllSettings())
	}

	viper.SetConfigFile(parser.tempPath)

	// Use viper to export config
	return viper.WriteConfig()
}

// For export config, after viper.WriteConfig() we traverse the config
// file and perform additional processing
// 1. Filer some items that we don't want to see in the config file
// 2. Add usage hint if any
func (parser *FlagsParser) beautifyPrintTemp() (err error) {
	defer os.Remove(parser.tempPath)

	var fh *os.File
	if fh, err = os.Open(parser.tempPath); err != nil {
		return fmt.Errorf("cannot open config file %s, %s", parser.tempPath, err)
	}
	defer fh.Close()

	scanner := bufio.NewScanner(fh)
	var currentSet string
	filterNextEmptyLine := true
	for scanner.Scan() {
		var usage string

		line := scanner.Text()
		lineT := strings.TrimSpace(line)
		if filterNextEmptyLine {
			filterNextEmptyLine = false
			if lineT == "" {
				continue
			}
		}
		if len(lineT) > 1 && lineT[0] == '[' && lineT[len(lineT)-1] == ']' {
			currentSet = lineT[1 : len(lineT)-1]
			goto OUTPUT
		}
		if strings.IndexByte(lineT, '=') > 0 {
			parts := strings.SplitN(lineT, "=", 2)
			k := strings.TrimSpace(parts[0])
			if len(k) > 0 {
				if filterItems.isItemIn(currentSet, k) {
					continue
				}
			}
			off := strings.IndexByte(line, lineT[0])
			flag := parser.findFlag(k)
			if flag != nil {
				usage = parser.indentUsage(off, flag.Usage)
				if !flag.Changed && !mandatoryItems.isItemIn(currentSet, k) {
					commentedLine := line[:off] + "# " + line[off:]
					line = commentedLine
				}
			}
		}

	OUTPUT:
		if usage != "" {
			fmt.Println(usage)
		}
		fmt.Println(line)
	}

	if err = scanner.Err(); err != nil {
		return fmt.Errorf("cannot read config file %s, %s", parser.tempPath, err)
	}

	return
}

func (parser *FlagsParser) indentUsage(offset int, help string) (usage string) {
	if len(help) == 0 {
		return
	}
	indent := ""
	usage = "\n" + help
	for i := 0; i < offset; i++ {
		indent += " "
	}
	indent += "## "
	usage = strings.ReplaceAll(usage, "\n", "\n"+indent)
	return
}

func (parser *FlagsParser) findFlag(name string) *pflag.Flag {
	for _, set := range parser.FlagSets {
		flag := set.FSet.Lookup(name)
		if flag != nil {
			return flag
		}
		for _, ss := range set.SubSet {
			flag := ss.FSet.Lookup(name)
			if flag != nil {
				return flag
			}
		}
	}
	return nil
}
