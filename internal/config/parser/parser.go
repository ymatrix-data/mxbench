package parser

import (
	"fmt"
	"math"
	"os"
	"strings"

	"github.com/spf13/pflag"

	"github.com/ymatrix-data/mxbench/internal/engine"
	"github.com/ymatrix-data/mxbench/internal/util"
	"github.com/ymatrix-data/mxbench/internal/util/mxerror"
)

type Parser struct {
	flags *FlagsParser
	env   *ENVParser
	cmd   *CmdParser
	file  *FileParser

	// Target
	cfg *engine.Config
}

func New(cfg *engine.Config) *Parser {
	parser := &Parser{
		cfg: cfg,
	}

	parser.flags = newFlagsParser(cfg)
	parser.env = newEnvParser(parser.flags)
	parser.cmd = newCmdParser(parser.env)
	parser.file = newFileParser(parser.cmd)

	return parser
}

// Parse command line flags first, then fill env into config secondary,
// if a --config <file> is given
// Will read from the config file and override config fields except those arguments
// listed in the command line.
// After then, all configuration info is extracted to the cfg struct
func (parser *Parser) Parse() {
	err := parser.flags.parse()
	parser.catch(err)

	err = parser.env.parse()
	parser.catch(err)

	err = parser.cmd.parse()
	parser.catch(err)

	err = parser.file.parse()
	parser.catch(err)
}

func (parser *Parser) catch(err error) {
	if err == nil {
		return
	}
	mxerr, ok := err.(*mxerror.MxbenchError)
	if !ok {
		fmt.Fprintln(os.Stderr, "Can not start mxbench:", err)
		os.Exit(1)
	}
	switch mxerr {
	case errParseFlags, errHelpWanted, errUnknown:
		mxerr.OSExit(func() {
			parser.flags.Usage()
		})
	case errVersionWanted:
		mxerr.OSExit(func() {
			fmt.Println("mxbench", util.GetMxbenchVersion())
		})
	case errConfigWanted:
		mxerr.OSExit(func() {
			err = parser.flags.Print()
			if err != nil {
				mxerr := mxerror.FromError(err)
				mxerr.OSExit()
			}
		})
	default:
		mxerr.OSExit()
	}
}
func (parser *FlagsParser) InitGeneratorFlagSet(cfg *engine.GeneratorConfig) (*pflag.FlagSet, []MatrixFlagSet) {
	const desc = `generator plugin is the data generator for mxbench
Types restricted to: telematics/nil
Sub-options varies based on generator type`

	parentSet := pflag.NewFlagSet("generator", pflag.ContinueOnError)
	parentSet.StringVar(&cfg.Plugin, "generator", "telematics", desc)
	parentSet.SortFlags = false
	parentSet.SetOutput(os.Stdout)

	// Try parse the type of generator first, and attach per plugin specific flags
	var v string
	var i, from int
	var to = math.MaxInt32
	for i, v = range os.Args[1:] {
		if strings.ToLower(v) == "--generator" {
			from = i + 1
			to = i + 3
			break
		}
	}

	if from >= len(os.Args) {
		from = len(os.Args) - 1
	}
	if to > len(os.Args) {
		to = len(os.Args)
	}
	_ = parentSet.Parse(os.Args[from:to])

	var pluginFlags *pflag.FlagSet
	var subSet []MatrixFlagSet
	pluginFlags, cfg.PluginConfig = parser.GetGeneratorDefaultFlagsFunc(*cfg)
	if pluginFlags != nil {
		pluginFlags.SortFlags = false
		pluginFlags.SetOutput(os.Stdout)
		subSet = append(subSet, MatrixFlagSet{"generator." + cfg.Plugin, pluginFlags, nil})
		parser.mainFlagSet.AddFlagSet(pluginFlags)
	}

	return parentSet, subSet
}

func (parser *FlagsParser) InitBenchmarkFlagSet(cfg *engine.BenchmarkConfig) (*pflag.FlagSet, []MatrixFlagSet) {
	const desc = `Benchmark generates or executes queries
Types restricted to: telematics/nil`
	parentSet := pflag.NewFlagSet("benchmark", pflag.ContinueOnError)
	parentSet.StringVar(&cfg.Plugin, "benchmark", "telematics", desc)
	parentSet.SortFlags = false
	parentSet.SetOutput(os.Stdout)

	// Try parse the type of benchmark first, and attach per plugin specific flags
	var v string
	var i, from int
	var to = math.MaxInt32
	for i, v = range os.Args[1:] {
		if strings.ToLower(v) == "--benchmark" {
			from = i + 1
			to = i + 3
			break
		}
	}

	if from >= len(os.Args) {
		from = len(os.Args) - 1
	}
	if to > len(os.Args) {
		to = len(os.Args)
	}
	_ = parentSet.Parse(os.Args[from:to])

	var pluginFlags *pflag.FlagSet
	var subSet []MatrixFlagSet
	pluginFlags, cfg.PluginConfig = parser.GetBenchmarkDefaultFlagsFunc(*cfg)
	if pluginFlags != nil {
		pluginFlags.SortFlags = false
		pluginFlags.SetOutput(os.Stdout)
		subSet = append(subSet, MatrixFlagSet{"benchmark." + cfg.Plugin, pluginFlags, nil})
		parser.mainFlagSet.AddFlagSet(pluginFlags)
	}

	return parentSet, subSet
}

func (parser *FlagsParser) InitWriterFlagSet(cfg *engine.WriterConfig) (*pflag.FlagSet, []MatrixFlagSet) {
	const desc = `Writer populates data to MatrixGate
Types restricted to: http/stdin/nil`

	parentSet := pflag.NewFlagSet("writer", pflag.ContinueOnError)
	parentSet.StringVar(&cfg.Plugin, "writer", "http", desc)
	parentSet.SortFlags = false
	parentSet.SetOutput(os.Stdout)

	// Try parse the type of writer first, and attach per plugin specific flags
	var v string
	var i, from int
	var to = math.MaxInt32
	for i, v = range os.Args[1:] {
		if strings.ToLower(v) == "--writer" {
			from = i + 1
			to = i + 3
			break
		}
	}

	if from >= len(os.Args) {
		from = len(os.Args) - 1
	}
	if to > len(os.Args) {
		to = len(os.Args)
	}
	_ = parentSet.Parse(os.Args[from:to])

	var pluginFlags *pflag.FlagSet
	var subSet []MatrixFlagSet
	pluginFlags, cfg.PluginConfig = parser.GetWriterDefaultFlagsFunc(*cfg)
	if pluginFlags != nil {
		pluginFlags.SortFlags = false
		pluginFlags.SetOutput(os.Stdout)
		subSet = append(subSet, MatrixFlagSet{"writer." + cfg.Plugin, pluginFlags, nil})
		parser.mainFlagSet.AddFlagSet(pluginFlags)
	}

	return parentSet, subSet
}
