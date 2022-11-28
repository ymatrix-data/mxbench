package cli

import (
	"fmt"
	"os"
	"os/user"
)

const DefaultHelpTemplate = `Usage of mxbench
%[4]s <command> [<args>]

The commands are:
    run            Run mxbench in command line
    config         Print full sample configuration to STDOUT
    help           Show usage
    version        Show version

The arguments are:

  Global Options:
      --cpu-profile                      turn on cpu-profile for mxbench
      --schema-name string               the name of the schema (default "public")
      --table-name string                the name of the table
      --tag-num int                      the number of the tags/devices (default 25000)
      --ts-start string                  the start timestamp; in 'realtime' mode it decides the the table's partition start time (default "2022-04-25 09:00:00")
      --ts-end string                    the end timestamp; in 'realtime' mode it decides the the table's partition end time (default "2022-04-25 09:03:00")
      --partition-interval-in-hour int   the interval of the partition (default 24)
      --realtime                         keep generating data using current time as timestamp until mxbench gets terminated manually
      --metrics-type string              the type of the metrics.
                                         Only support "int4", "int8", "float4", "float8", 4 types in total. (default "float8")
      --ts-step-in-second uint           the step of timestamp (default 1)
      --total-metrics-count int          the total count of metrics.
                                         If it is greater than 998, then the exceeding metrics will be assigned into a JSON column named "ext".
                                         e.g. If it is given 1000 metrics, then 2 of them can be retrieved in column "ext" of JSON. (default 300)
      --metrics-descriptions string      metrics-descriptions
      --dump                             whether to dump or not
      --simultaneous-loading-and-query   whether to load data and run benchmark queries simultaneously
      --workspace string                 the workspace of mxbench, directory to dump or backup (default "/tmp/mxbench")
      --ddl-file-path string             the file path of ddl
  -C, --config string                    configuration file to load
      --skip-set-gucs                    whether to skip set GUCs
      --log-level string                 log level. support "debug", "verbose", "info", "error" (default "info")
  -h, --help                             print usage
      --version                          print version
      --watch                            whether to watch progress of each step or not (default true)
      --report-format string             the format of report (default "csv")
      --report-path string               the file path for report to dump (default "/tmp")

  Database Options:
      --db-master-host string   The hostname of YMatrix master (default "%[1]s")
      --db-master-port int      The port of YMatrix master (default 5432)
      --db-user string          The user name of YMatrix (default "%[2]s")
      --db-password string      Password of YMatrix user
      --db-database string      The database name of YMatrix (default "%[3]s")

  Generator Options:
      --generator string   generator plugin is the data generator for mxbench
                           Types restricted to: telematics/nil
                           Sub-options varies based on generator type (default "telematics")
%[5]s
  Benchmark Options:
      --benchmark string   Benchmark generates or executes queries
                           Types restricted to: telematics/nil (default "telematics")
%[7]s
  Writer Options:
      --writer string   Writer populates data to MatrixGate
                        Types restricted to: http/stdin/nil (default "http")
      --writer-progress-format string        progress format, support "list", "json" (default "list")
      --writer-progress-include-table-size   whether progress include table size
      --writer-progress-with-timezone        whether print time with timezone
%[6]s
Examples:

    # generate a mxbench config file with given args:
    %[4]s config [<args...>] > mxbench.conf

    # example to generate with listed argements, all non-listed values be default:
    %[4]s config --db-master-port 6000 > mxbench.conf

    # edit mxbench.conf with your customized configuration for each plugin:
    # such as delimiter or time format
    vim mxbench.conf

    # launch mxbench with the config file:
    %[4]s --config mxbench.conf

    # launch mxbench with the config file, override a handful of args:
    %[4]s run --config mxbench.conf --db-master-port 7000

    # launch mxbench without a config file:
    %[4]s run [<args...>]
`

func TelematicsgGeneratorStdinWriterNilBenchmarkUsage() string {
	hostname, _ := os.Hostname()
	pgUser := os.Getenv("PGUSER")
	u, _ := user.Current()
	if u != nil {
		if pgUser == "" {
			pgUser = u.Username
		}
	}
	pgDatabase := os.Getenv("PGDATABASE")
	if pgDatabase == "" {
		pgDatabase = "postgres"
	}

	return fmt.Sprintf(DefaultHelpTemplate, hostname, pgUser, pgDatabase, "mxbench_e2e", `      --generator-disorder-ratio int      The percent of data timestamp that is disordered.
                                          Expected to be an integer ranging from 0 to 100 (included).
      --generator-batch-size int          The number of lines of data generated for a tag of a given timestamp.
                                          e.g. It is set to be 5, then for tag "tag1" with ts of "2022-04-02 15:04:03",
                                          5 lines of data will be generated and sent into DBMS.
                                          Eventually, however, they will be merged as 1 row in DBMS. (default 1)
      --generator-randomness string       The randomness of metrics, OFF/S/M/L (default "OFF")
      --generator-empty-value-ratio int   the ratio of empty metrics value in one line.
                                          Expected to be an integer ranging from 0 to 100 (included). (default 90)
`, ``, ``)
}

func NilGeneratorStdinWriterNilBenchmarkUsage() string {
	hostname, _ := os.Hostname()
	pgUser := os.Getenv("PGUSER")
	u, _ := user.Current()
	if u != nil {
		if pgUser == "" {
			pgUser = u.Username
		}
	}
	pgDatabase := os.Getenv("PGDATABASE")
	if pgDatabase == "" {
		pgDatabase = "postgres"
	}

	return fmt.Sprintf(DefaultHelpTemplate, hostname, pgUser, pgDatabase, "mxbench_e2e", ``, ``, ``)
}
