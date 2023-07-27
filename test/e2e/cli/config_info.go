package cli

import (
	"fmt"
	"os"
	"os/user"
	"strconv"
)

const DefaultConfigTemplate = `[benchmark]

  ## Benchmark generates or executes queries
  ## Types restricted to: telematics/nil
  benchmark = "telematics"

  [benchmark.telematics]

    ## Queries by combing expressions
    # benchmark-combination-queries = ""

    ## custom query SQLs, use "," to separate query statements.
    ## For example, ["SELECT COUNT(*) from t1", "SELECT MAX(ts) from t1"]
    # benchmark-custom-queries = []

    ## the count of metrics of json type, max is 1600
    # benchmark-json-metrics-count = 10

    ## parallels of benchmark,use "," to run queries with different concurrency.
    ## For example, input [1, 8] to run queries with parallel of 1 and 8 respectively.
    # benchmark-parallel = []

    ## progress format. support "list", "json"
    # benchmark-progress-format = "list"

    ## query names to be run, the default includes none of telematics benchmark queries.
    ## Please input the query names, and use "," to separate query names.
    ## For example, input:
    ## [ "SINGLE_TAG_LATEST_QUERY", "MULTI_TAG_LATEST_QUERY", "SINGLE_TAG_DETAIL_QUERY" ]
    ## to run all telematics queries, or a subset of it.
    ## Any other arbitrary query name will be skipped.
    ## The order matters.
    # benchmark-run-query-names = []

    ## the times of queries with set parallels
    # benchmark-run-times = 0

    ## total runtime of queries, only take effect when benchmark-run-times is 0
    # benchmark-runtime-in-second = "60"

    ## the count of metrics of simple type, max is 998
    # benchmark-simple-metrics-count = 290

    ## the end timestamp of query
    # benchmark-ts-end = ""

    ## the start timestamp of query
    # benchmark-ts-start = ""

[database]

  ## The database name of YMatrix
  # db-database = "%[1]s"

  ## The hostname of YMatrix master
  # db-master-host = "%[2]s"

  ## The port of YMatrix master
  # db-master-port = %[4]d

  ## Password of YMatrix user
  # db-password = ""

  ## The user name of YMatrix
  # db-user = "%[3]s"

[generator]

  ## generator plugin is the data generator for mxbench
  ## Types restricted to: telematics/nil
  ## Sub-options varies based on generator type
  generator = "telematics"

  [generator.telematics]

    ## The number of lines of data generated for a tag of a given timestamp.
    ## e.g. It is set to be 5, then for tag "tag1" with ts of "2022-04-02 15:04:03",
    ## 5 lines of data will be generated and sent into DBMS.
    ## Eventually, however, they will be merged as 1 row in DBMS.
    # generator-batch-size = 1

    ## The percent of data timestamp that is disordered.
    ## Expected to be an integer ranging from 0 to 100 (included).
    # generator-disorder-ratio = 0

    ## the ratio of empty metrics value in one line.
    ## Expected to be an integer ranging from 0 to 100 (included).
    # generator-empty-value-ratio = 90

    ## num of goroutines that it will use to call write function
    # generator-num-goroutine = 1

    ## The randomness of metrics, OFF/S/M/L
    # generator-randomness = "OFF"

    ## the estimated mega bytes of batch size to call write function
    # generator-write-batch-size = 4

[global]

  ## configuration file to load
  # config = ""

  ## turn on cpu-profile for mxbench
  # cpu-profile = false

  ## the file path of ddl
  # ddl-file-path = ""

  ## whether to dump or not
  # dump = false

  ## print usage
  # help = false

  ## log level. support "debug", "verbose", "info", "error"
  # log-level = "info"

  ## metrics-descriptions
  # metrics-descriptions = ""

  ## the type of the metrics.
  ## Only support "int4", "int8", "float4", "float8", 4 types in total.
  # metrics-type = "float8"

  ## the interval of the partition
  # partition-interval-in-hour = 24

  ## some specific sql such as analyze and vaccum database before run benchmark queries
  # pre-benchmark-query = ""

  ## keep generating data using current time as timestamp until mxbench gets terminated manually
  # realtime = false

  ## the format of report
  # report-format = "csv"

  ## the file path for report to dump
  # report-path = "/tmp"

  ## the name of the schema
  # schema-name = "public"

  ## whether to load data and run benchmark queries simultaneously
  # simultaneous-loading-and-query = false

  ## whether to skip set GUCs
  # skip-set-gucs = false

  ## storage type
  # storage-type = "mars3"

  ## the name of the table
  # table-name = ""

  ## the number of the tags/devices
  # tag-num = 25000

  ## the total count of metrics.
  ## If it is greater than 998, then the exceeding metrics will be assigned into a JSON column named "ext".
  ## e.g. If it is given 1000 metrics, then 2 of them can be retrieved in column "ext" of JSON.
  # total-metrics-count = 300

  ## the end timestamp; in 'realtime' mode it decides the the table's partition end time
  # ts-end = "2022-04-25 09:03:00"

  ## the start timestamp; in 'realtime' mode it decides the the table's partition start time
  # ts-start = "2022-04-25 09:00:00"

  ## the step of timestamp
  # ts-step-in-second = "1"

  ## print version
  # version = false

  ## whether to watch progress of each step or not
  # watch = true

  ## the workspace of mxbench, directory to dump or backup
  # workspace = "/tmp/mxbench"

%[5]s
`

func DefaultConfig() string {
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
	pgPort := 5432
	pgPortStr := os.Getenv("PGPORT")
	if pgPortStr != "" {
		i, err := strconv.Atoi(pgPortStr)
		if err == nil && i > 0 {
			pgPort = i
		}
	}

	return fmt.Sprintf(DefaultConfigTemplate, pgDatabase, hostname, pgUser, pgPort, `[writer]

  ## Writer populates data to MatrixGate
  ## Types restricted to: http/stdin/nil
  writer = "http"

  [writer.http]

    ## interval for mxgate
    # writer-interval = -1

    ## path of mxgate
    # writer-mxgate-path = ""

    ## The parallel of http writer
    # writer-parallel = 8

    ## progress format. support "list", "json"
    # writer-progress-format = "list"

    ## whether progress include table size
    # writer-progress-include-table-size = false

    ## whether print time with timezone
    # writer-progress-with-timezone = false

    ## stream-prepared for mxgate
    # writer-stream-prepared = -1

    ## use gzip for http writer
    # writer-use-gzip = false`)
}
