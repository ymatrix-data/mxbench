package file

import (
	"bufio"
	"bytes"
	"context"
	"os"

	"github.com/spf13/pflag"

	"github.com/ymatrix-data/mxbench/internal/engine"
	"github.com/ymatrix-data/mxbench/internal/engine/metadata"
	"github.com/ymatrix-data/mxbench/internal/util/log"
)

const (
	bufferSize = 4 * 1024 * 1024
	batchLimit = 3 * 1024 * 1024
)

type Config struct {
	FilePaths       []string `mapstructure:"generator-file-paths"`
	BatchSize       int      `mapstructure:"generator-batch-size"`
	EmptyValueRatio int      `mapstructure:"generator-empty-value-ratio"`
}

type Generator struct {
	ctx        context.Context
	cancelFunc context.CancelFunc
	cfg        *Config
}

func NewGenerator(cfg engine.GeneratorConfig) engine.IGenerator {
	gCfg := cfg.PluginConfig.(*Config)

	ctx, cancel := context.WithCancel(context.Background())
	return &Generator{
		cfg:        gCfg,
		ctx:        ctx,
		cancelFunc: cancel,
	}
}

func (g *Generator) Run(_ engine.GlobalConfig, _ *metadata.Metadata, writeFunc engine.WriteFunc) error {
	if len(g.cfg.FilePaths) == 0 {
		log.Warn("[Generator.FILE] No path for data files is assigned to 'generator-file-paths', skipping...")
		return nil
	}
	dataFiles := make([]*os.File, 0, len(g.cfg.FilePaths))
	for _, filePath := range g.cfg.FilePaths {
		dataFile, err := os.OpenFile(filePath, os.O_RDONLY, 0400)
		if err != nil {
			return err
		}
		dataFiles = append(dataFiles, dataFile)
	}
	defer func() {
		for _, file := range dataFiles {
			file.Close()
		}
	}()

	log.Info("[Generator.FILE] Start to load to writer")
	defer log.Info("[Generator.FILE] Data generating ended")

	for _, dataFile := range dataFiles {
		scanner := bufio.NewScanner(dataFile)
		// TODO: resize scanner's capacity for lines over 64K
		buff := bytes.NewBuffer(make([]byte, 0, bufferSize))
		var batchLine int64
		for scanner.Scan() {
			if buff.Len() > batchLimit {
				select {
				case <-g.ctx.Done():
					return nil
				default:
					err := writeFunc(buff.Bytes(), batchLine, int64(buff.Len()))
					if err != nil {
						return err
					}
					buff.Reset()
					batchLine = 0
				}
			}
			buff.WriteString(scanner.Text())
			buff.WriteByte('\n')
			batchLine++
		}
		// last flush
		err := writeFunc(buff.Bytes(), batchLine, int64(buff.Len()))
		if err != nil {
			return err
		}
		if err = scanner.Err(); err != nil {
			return err
		}
	}
	return nil
}

func (g *Generator) GetPrediction(_ *metadata.Table) (engine.GeneratorPrediction, error) {
	var totalSize int64
	for _, filePath := range g.cfg.FilePaths {
		fileInfo, err := os.Stat(filePath)
		if err != nil {
			return engine.GeneratorPrediction{}, err
		}

		totalSize += fileInfo.Size()
	}
	return engine.GeneratorPrediction{Size: totalSize}, nil
}

func (g *Generator) ModifyMetadataConfig(metaConfig *metadata.Config) {
	if metaConfig == nil {
		return
	}
	metaConfig.HasUniqueConstraints = g.cfg.BatchSize > 1
	metaConfig.EmptyValueRatio = 100 - g.cfg.BatchSize*(100-g.cfg.EmptyValueRatio)
	if metaConfig.EmptyValueRatio < 0 {
		metaConfig.EmptyValueRatio = 0
	}
	if metaConfig.EmptyValueRatio > 100 {
		metaConfig.EmptyValueRatio = 100
	}
}

func (g *Generator) Close() error {
	g.cancelFunc()
	return nil
}

func (g *Generator) CreatePluginConfig() interface{} {
	return &Config{}
}

func (g *Generator) GetDefaultFlags() (*pflag.FlagSet, interface{}) {
	gCfg := &Config{}
	p := pflag.NewFlagSet("generator.file", pflag.ContinueOnError)
	p.StringSliceVar(&gCfg.FilePaths, "generator-file-paths", []string{}, "the absolute paths of the data file to be loaded")
	p.IntVar(&gCfg.BatchSize, "generator-batch-size", 1, "The number of lines of data generated for a tag of a given timestamp.\n"+
		"e.g. It is set to be 5, then for tag \"tag1\" with ts of \"2022-04-02 15:04:03\",\n5 lines of data will be generated and sent into DBMS.\n"+
		"Eventually, however, they will be merged as 1 row in DBMS.")
	p.IntVar(&gCfg.EmptyValueRatio, "generator-empty-value-ratio", 90, "the ratio of empty metrics value in one line.\n"+
		"Expected to be an integer ranging from 0 to 100 (included).")
	return p, gCfg
}

func (g *Generator) IsNil() bool {
	return g == nil
}
