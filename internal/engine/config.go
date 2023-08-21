package engine

import (
	"fmt"
	"os"
	"os/user"
	"time"

	"github.com/spf13/pflag"

	"github.com/ymatrix-data/mxbench/internal/engine/metadata"
	"github.com/ymatrix-data/mxbench/internal/util"
	"github.com/ymatrix-data/mxbench/internal/util/mxerror"
)

type ConfigurablePlugin interface {
	// CreatePluginConfig returns a plugin specific config struct, can have any k=>v pairs
	// It is plugin author's responsibility to tag "`mapstructure:"<key-name>"`" on struct members
	// to make it able to export/import with config file
	CreatePluginConfig() interface{}

	// DefaultConfig returns a viper struct which have all default k=>v set using viper.Set() function
	// Those key names should be consistent with tags "`mapstructure:"<key-name>"`" on the config struct
	// DefaultConfig(*viper.Viper)

	// GetDefaultFlags returns a flagSet struct which have all flags with specified name, value, and usage string.
	// Those key names should be consistent with tags "`mapstructure:"<key-name>"`" on the config struct
	// The second return value is the plugin config struct created inside this function
	GetDefaultFlags() (*pflag.FlagSet, interface{})
}

type NewGeneratorFunc func(cfg GeneratorConfig) IGenerator
type NewWriterFunc func(cfg WriterConfig) IWriter
type NewBenchmarkFunc func(cfg BenchmarkConfig) IBenchmark

type GlobalConfig struct {
	SchemaName               string               `mapstructure:"schema-name"`
	TableName                string               `mapstructure:"table-name"`
	TagNum                   int64                `mapstructure:"tag-num"`
	TimestampStart           string               `mapstructure:"ts-start"`
	TimestampEnd             string               `mapstructure:"ts-end"`
	PartitionIntervalInHour  int64                `mapstructure:"partition-inteval-in-hour"`
	IsRealtimeMode           bool                 `mapstructure:"realtime"`
	MetricsType              metadata.MetricsType `mapstructure:"metrics-type"`
	TotalMetricsCount        int64                `mapstructure:"total-metrics-count"`
	TimestampStepInSecond    uint64               `mapstructure:"ts-step-in-second"`
	Dump                     bool                 `mapstructure:"dump"`
	MetricsDescriptions      string               `mapstructure:"metrics-descriptions"`
	Workspace                string               `mapstructure:"workspace"`
	DDLFilePath              string               `mapstructure:"ddl-file-path"`
	SimultaneousLoadAndQuery bool                 `mapstructure:"simultaneous-loading-and-query"`
	PreBenchmarkQuery        string               `mapstructure:"pre-benchmark-query"`
	PreBenchmarkCmd          string               `mapstructure:"pre-benchmark-command"`
	SkipSetGUCs              bool                 `mapstructure:"skip-set-gucs"`
	StorageType              string               `mapstructure:"storage-type"`

	// misc
	Command       string
	CfgFile       string
	HelpWanted    bool   `mapstructure:"help"`
	VersionWanted bool   `mapstructure:"version"`
	LogLevel      string `mapstructure:"log-level"`
	Watch         bool   `mapstructure:"watch"`
	CPUProfile    bool

	StartAt time.Time
	EndAt   time.Time

	ReportPath   string       `mapstructure:"report-path"`
	ReportFormat ReportFormat `mapstructure:"report-format"`
}

func (cfg *GlobalConfig) NewMetadataConfig() *metadata.Config {
	return &metadata.Config{
		SchemaName:              cfg.SchemaName,
		TableName:               cfg.TableName,
		TagNum:                  cfg.TagNum,
		StartAt:                 cfg.StartAt,
		EndAt:                   cfg.EndAt,
		PartitionIntervalInHour: cfg.PartitionIntervalInHour,
		MetricsType:             cfg.MetricsType,
		TotalMetricsCount:       cfg.TotalMetricsCount,
		MetricsDescriptions:     cfg.MetricsDescriptions,
		TimestampStepInSecond:   cfg.TimestampStepInSecond,
		StorageType:             cfg.StorageType,
		IsDDLFromFile:           cfg.DDLFilePath != "",
	}
}

func (cfg *GlobalConfig) DoAfterInit() error {
	var err error

	cfg.StartAt, err = time.Parse(util.TIME_FMT, cfg.TimestampStart)
	if err != nil {
		return err
	}
	cfg.EndAt, err = time.Parse(util.TIME_FMT, cfg.TimestampEnd)
	if err != nil {
		return err
	}

	if cfg.StartAt.After(cfg.EndAt) {
		return mxerror.CommonErrorf("ts-start(%s) is after ts-end(%s)", cfg.TimestampStart, cfg.TimestampEnd)
	}
	return err
}

type Config struct {
	GlobalCfg    GlobalConfig      `mapstructure:"global"`
	DB           util.DBConnParams `mapstructure:"database"`
	GeneratorCfg GeneratorConfig   `mapstructure:"generator"`
	WriterCfg    WriterConfig      `mapstructure:"writer"`
	BenchmarkCfg BenchmarkConfig   `mapstructure:"benchmark"`

	// print config usage
	Usage func()

	NewGeneratorFunc NewGeneratorFunc
	NewWriterFunc    NewWriterFunc
	NewBenchmarkFunc NewBenchmarkFunc
}

func NewConfig() (*Config, error) {
	hostname, err := os.Hostname()
	if err != nil {
		return nil, err
	}

	var cfg = &Config{}

	pgUser := os.Getenv("PGUSER")
	u, _ := user.Current()
	if u != nil {
		if pgUser == "" {
			pgUser = u.Username
		}
	}
	cfg.DB.MasterHost = hostname
	cfg.DB.User = pgUser
	return cfg, nil
}

func (cfg *Config) GlobalFlagSet() *pflag.FlagSet {
	set := pflag.NewFlagSet("global", pflag.ContinueOnError)
	set.BoolVar(&cfg.GlobalCfg.CPUProfile, "cpu-profile", false, "turn on cpu-profile for mxbench")
	set.StringVar(&cfg.GlobalCfg.SchemaName, "schema-name", "public", "the name of the schema")
	set.StringVar(&cfg.GlobalCfg.TableName, "table-name", "", "the name of the table")
	set.Int64Var(&cfg.GlobalCfg.TagNum, "tag-num", 25000, "the number of the tags/devices")
	set.StringVar(&cfg.GlobalCfg.TimestampStart, "ts-start", "2022-04-25 09:00:00", "the start timestamp; in 'realtime' mode it decides the the table's partition start time")
	set.StringVar(&cfg.GlobalCfg.TimestampEnd, "ts-end", "2022-04-25 09:03:00", "the end timestamp; in 'realtime' mode it decides the the table's partition end time")
	set.Int64Var(&cfg.GlobalCfg.PartitionIntervalInHour, "partition-interval-in-hour", 24, "the interval of the partition")
	set.BoolVar(&cfg.GlobalCfg.IsRealtimeMode, "realtime", false, "keep generating data using current time as timestamp until mxbench gets terminated manually")

	set.StringVar(&cfg.GlobalCfg.MetricsType, "metrics-type", "float8", "the type of the metrics.\n"+
		"Only support \"int4\", \"int8\", \"float4\", \"float8\", 4 types in total.")
	set.Uint64Var(&cfg.GlobalCfg.TimestampStepInSecond, "ts-step-in-second", 1, "the step of timestamp")
	set.Int64Var(&cfg.GlobalCfg.TotalMetricsCount, "total-metrics-count", 300, fmt.Sprintf("the total count of metrics.\n"+
		"If it is greater than %[1]d, then the exceeding metrics will be assigned into a JSON column named \"%[3]s\".\n"+
		"e.g. If it is given %[2]d metrics, then 2 of them can be retrieved in column \"%[3]s\" of JSON.",
		metadata.MAX_SIMPLE_COLUMN_NUM-metadata.NON_METRICS_COLUMN_NUM,
		metadata.MAX_SIMPLE_COLUMN_NUM-metadata.NON_METRICS_COLUMN_NUM+2,
		metadata.ColumnNameExt,
	))
	set.StringVar(&cfg.GlobalCfg.StorageType, "storage-type", "mars3", "supported storage types are mars2, mars3")

	// TODO complete the hint info
	set.StringVar(&cfg.GlobalCfg.MetricsDescriptions, "metrics-descriptions", "", "metrics-descriptions")
	set.BoolVar(&cfg.GlobalCfg.Dump, "dump", false, "whether to dump or not")
	set.BoolVar(&cfg.GlobalCfg.SimultaneousLoadAndQuery, "simultaneous-loading-and-query", false,
		"whether to load data and run benchmark queries simultaneously")
	set.StringVar(&cfg.GlobalCfg.Workspace, "workspace", "/tmp/mxbench", "the workspace of mxbench, directory to dump or backup")
	set.StringVar(&cfg.GlobalCfg.DDLFilePath, "ddl-file-path", "", "the file path of ddl")
	set.StringVarP(&cfg.GlobalCfg.CfgFile, "config", "C", "", "configuration file to load")
	set.BoolVar(&cfg.GlobalCfg.SkipSetGUCs, "skip-set-gucs", false, "whether to skip set GUCs")
	set.StringVar(&cfg.GlobalCfg.PreBenchmarkQuery, "pre-benchmark-query", "", "some specific sql such as analyze and vaccum database before run benchmark queries")
	set.StringVar(&cfg.GlobalCfg.PreBenchmarkCmd, "pre-benchmark-command", "", "some specific command to execute before run benchmark queries")

	// misc
	set.StringVar(&cfg.GlobalCfg.LogLevel, "log-level", "info", "log level. support \"debug\", \"verbose\", \"info\", \"error\"")

	set.BoolVarP(&cfg.GlobalCfg.HelpWanted, "help", "h", false, "print usage")
	set.BoolVar(&cfg.GlobalCfg.VersionWanted, "version", false, "print version")
	set.BoolVar(&cfg.GlobalCfg.Watch, "watch", true, "whether to watch progress of each step or not")
	set.StringVar(&cfg.GlobalCfg.ReportFormat, "report-format", "csv", "the format of report")
	set.StringVar(&cfg.GlobalCfg.ReportPath, "report-path", "/tmp", "the file path for report to dump")
	set.SortFlags = false
	set.SetOutput(os.Stdout)
	return set
}

func (cfg *Config) DBFlagSet() *pflag.FlagSet {
	set := pflag.NewFlagSet("database", pflag.ContinueOnError)
	set.StringVar(&cfg.DB.MasterHost, "db-master-host", cfg.DB.MasterHost, "The hostname of YMatrix master")
	set.IntVar(&cfg.DB.MasterPort, "db-master-port", 5432, "The port of YMatrix master")
	set.StringVar(&cfg.DB.User, "db-user", cfg.DB.User, "The user name of YMatrix")
	set.StringVar(&cfg.DB.Password, "db-password", "", "Password of YMatrix user")
	set.StringVar(&cfg.DB.Database, "db-database", "postgres", "The database name of YMatrix")
	set.SortFlags = false
	set.SetOutput(os.Stdout)
	return set
}
