# mxbench
mxbench 是YMatrix数据加载和查询的压测工具，可以根据用户给定的设备数量、时间范围、指标数量配置快速生成随机数据，自动创建数据表，串行或并发进行数据加载和查询。用户可以灵活配置指标类型、采集频率、空值率、随机度等，还可以指定查询的线程数、定制查询语句等。mxbench可以通过命令行运行，也可以通过配置文件运行。该工具位于YMatrix安装目录下的bin/mxbench。

## 0.准备

### 1.1 YMatrix集群
需要一个正常运行的YMatrix集群。

### 1.2 环境变量
由于mxbench需要调用createdb, gpconfig, gpstop，因此需要用户配置好相关环境变量使这些命令可以正确执行。

具体的，需要执行source `<YMatrix安装目录>/greenplum_path.sh`， 还需要正确设置以下环境变量：
- PGHOST
- PGPORT
- PGUSER
- PGPASSWORD
- PGDATABASE
- MASTER_DATA_DIRECTORY

此外，用户还可以试执行 `createdb mxbench`, `gpconfig -s log_rotation_size`, `gpstop -rai`等命令，确保可以正确运行。

### 1.3 mxgate
需要通过mxgate加载数据。mxgate，是一款高性能流式数据加载服务器，位于 YMatrix 安装目录下的 bin/mxgate。
更多相关信息请见 [mxgate](https://ymatrix.cn/doc/latest/datainput/matrixgate.md)。

## 1.构建

安装 GO 1.19

```bash
$ git clone git@github.com:ymatrix-data/mxbench.git
$ cd ./mxbench
$ go mod tidy
$ make build
```

运行行为测试
```bash
$ make e2e
```


另外，mxbench工具也随YMatrix安装包(>= 4.5.0)一同提供。
```
$ cd <YMatrix安装目录>
```

该工具位于YMatrix安装目录下的bin/mxbench。


## 2.用法

### 2.1 快速试用

如果想在个人开发机上快速试用mxbench，可以使用配置文件或者命令行的方式运行mxbench.

#### 2.1.1 配置文件

可以使用以下配置文件，命名为`mxbench.conf`，并运行:
`./bin/mxbench --config mxbench.conf`.
注意`benchmark-parallel`参数设置需要适应机器性能，建议小于或等于CPU核数。

```toml
[database]
  db-database = "testdb1"
  db-master-port = 5432

[global]
  # 略过询问是否重设GUCs步骤
  skip-set-gucs = true

  table-name = "table1"

[benchmark]
  benchmark = "telematics"

  [benchmark.telematics]
    # 数组，查询并发度
    benchmark-parallel = [8]
    # 跑一个查询：单车明细查询
    benchmark-run-query-names = ["SINGLE_TAG_DETAIL_QUERY" ]
    # 每轮每条query的跑的次数或时间，让时间生效需要将次数设置为0，如下：
    benchmark-run-times = 0
    benchmark-runtime-in-second = "30"
```

#### 2.1.2 命令行

还可以使用命令行运行mxbench。运行以下命令就等同于以上面的配置文件运行mxbench。

```bash
./bin/mxbench run \
  --db-database "testdb1" \
  --db-master-host "localhost" \
  --db-master-port 5432 \
  --db-user "mxadmin" \
  --skip-set-gucs \
  --table-name "table1" \
  --benchmark "telematics" \
  --benchmark-run-query-names "SINGLE_TAG_DETAIL_QUERY" \
  --benchmark-parallel 8 \
  --benchmark-run-times 0 \
  --benchmark-runtime-in-second 30
```

### 2.2 配置详解

从示例配置文件可以看到，配置文件分为以下几个板块：

全局配置

1. database: 数据库相关的配置；
2. global: 包括一些表结构、数据量信息，以及是否dump数据文件或实际执行测试、是否开启进度观察等管理方面的配置；

可插件化的配置

3. generator: 数据生成器：
* [telematics](internal/engine/generator/telematics)(默认) - 生成车联网场景的数据；
* [file](internal/engine/generator/file) - 从文件中读取数据；
* [nil](internal/engine/generator/nil) - 不生成数据。

4. writer: 负责把生成的数据通过mxgate写入YMatrix：
* [http](internal/engine/writer/http)(默认) - 以HTTP的方式启动mxgate，并通过其加载数据至YMatrix;
* [stdin](internal/engine/writer/stdin) - 以stdin的方式启动mxgate，并通过其加载数据至YMatrix;
* [nil](internal/engine/writer/nil) - 不启动mxgate，不加载。

5. benchmark: 可插件化，生成并执行query语句：
* [telematics](internal/engine/benchmark/telematics)(默认) - 生成并执行车辆网场景下的常用query，支持定制query；
* [nil](internal/engine/benchmark/nil) - 不生成、不执行query语句。

以下是各模块参数详解：

#### 2.2.1 database
```toml
[database]
  # 数据库名称，如果不存在，mxbench会自动创建；已存在也不会报错  
  db-database = "postgres"

  ## YMatrix master实例所在主机的hostname
  db-master-host = "localhost"

  ## YMatrix master 实例的端口号
  db-master-port = 5432

  # YMatrix 用户密码
  db-password = ""

  # YMatrix 用户名
  db-user = "mxadmin"
```

#### 2.2.2 global

```toml
[global]

  # mxbench 生成DDL文件，现有系统GUCs恢复脚本， 最佳实践推荐GUCs更改脚本，
  # csv数据文件，query文件的目录。
  # 如果不存在，mxbench会自动创建；如果已存在且非目录，则会报错。可能需要注意权限问题。
  # 每次运行mxbench，都会在其下创建名为Unix时间戳的目录，该次运行生成的文件都会在该目录下。
  # 默认为"/tmp/mxbench"
  workspace = "/tmp/mxbench"

  # 是否dump csv数据文件，可选true或false。
  # 默认为false，会执行DDL、数据加载、query。
  # 在workspace下Unix时间戳的目录中，会生成：DDL、GUCs相关脚本和query文件。
  # 1. mxbench_ddl.sql: DDL文件；
  # 2. mxbench_gucs_setup.sh: 最佳实践推荐的GUCs设置脚本，可能需要再重启YMatrix才能全部生效。
  # 3. mxbench_gucs_backup.sh: 现有系统GUCs备份。如果现有系统的GUCs和最佳实践的GUCs一致，便不会生成。
  # 4. mxbench_<benchmark-plugin>_query.sql: 相应benchmark插件生成的query语句。
  # 如果选择true，则不会实际执行DDL、数据加载以及query；除了上面的各文件，还会生成：
  # 5. mxbench_<generator-plugin>_data.csv: 相应generator插件生成的data的csv文件。
  dump = false

  # 是否开启进程观察，可选true或false。
  #（默认）如果选择true，则会每5秒打印一次writer和benchmark模块的执行进度信息。
  # 选择false关闭。
  watch = true

  # log 级别，支持 "debug", "verbose", "info", "error"，默认为 "info"
  # log-level = "info"

  # 跳过询问和设置GUCs, 默认false，即mxbench会询问是否重设并重启数据库。
  # 如果不跳过，mxbench会根据本配置文件呈现出的数据特征，选择一个合理的GUCs配置。
  # skip-set-gucs = false

  # 是否同时执行数据加载和查询，可选true或false。
  # 如果选择true: 执行数据加载和查询的混合负载
  # query跑完之后会再循环跑，直到数据加载结束。
  # （默认）如果选择false: 先执行数据加载，执行完毕后再执行查询。
  simultaneous-loading-and-query = false

  # 如果需要定制DDL，该参数填写DDL文件的路径。
  # （默认）不填写则会根据其他相关配置生成DDL。
  ddl-file-path = ""

  # 指标的类型。默认为"float8"，即双精度浮点数。
  # 只支持 "int4", "int8", "float4", "float8", 4种类型.
  metrics-type = "float8"

  # 对指标的表述，有一套自己的语法
  # 如果不为空字符串，则可以生成特征的指标数据，
  # 指定详见本文档的“多数据类型与特征”板块。
  # metrics-descriptions = ""

  # 生成表DDL的分区interval，单位为h, 即小时，默认为24，即一天.
  # 如果让mxbench指定，则设为0即可。
  # partition-interval-in-hour = 24

  # 实时模式，默认false。
  # 若指定为true，则该板块的 ts-start 和 ts-end 将失效。
  # 数据时间为从本次mxbench运行后、开始进行数据生成与加载的自然时间，
  # 直到手动停止mxbench才会结束。
  # realtime = false

  # 生成SQL执行各项数据报的格式，支持"csv"格式，即默认值。
  # report-format = "csv"

  # 生成SQL执行各项数据报的路径，最终会生成在该路径下名为report.csv的报告。
  # report-path = "/tmp"

  # schema名称，默认为"public".
  schema-name = "public"

  # 数据表名称。默认为"", 必须手动设置。
  # 如果该同名表在配置的数据库、schema下存在，会报错，终止mxbench程序。
  table-name = "test_table"

  # 设备数量。默认25000.
  tag-num = 25000

  # 指标总数。默认300.
  # 如果指标总数大于998，则超过部分的指标会以json类型存放在名为"ext"的列中。
  # 例如，如果指标总数设为1000，则998个会以简单列形式存放在名为c0~c997的列中，
  # 其他2个以json形式放在ext的json类型的列中。
  total-metrics-count = 300

  # 生成数据时间戳起始时间。因为有延迟上报的数据存在，生成数据可能会早于这个时间。
  ts-start = "2022-04-25 09:00:00"

  # 生成数据时间戳终止时间。ts-end必须晚于ts-start，否则报错
  ts-end = "2022-04-25 09:01:00"

  # 每几秒采集一次指标。默认为"1"。
  ts-step-in-second = "1"
```

#### 2.2.3 generator

##### 2.2.3.1 telematics(默认)

生成车联网场景数据，选择generator="telematics"。

```toml
[generator]

  generator = "telematics"

  [generator.telematics]

    # 每个设备在每个时间戳的指标，分几条上传至YMatrix。
    # 例如，如果设置为5，名为"tag1"的设备在"2022-04-25 09:00:03"这个时间戳下，
    # 各个指标的数据会分5条传到YMatrix，最终合并为1个tuple.
    # 默认为1. 即指标信息不做拆分。
    generator-batch-size = 1

    # 延迟上报数据的比例。取值1～100.
    # 默认值为0, 即没有延迟上报的数据。
    generator-disorder-ratio = 0

    # 每行数据的空值率。取值为1～100. 默认为90%，即90%的指标都将是空值。
    generator-empty-value-ratio = 90

    # 指标数据随机度, 分为OFF/S/M/L四档。默认为OFF。
    generator-randomness = "OFF"

    # 生成数据的使用并发数，默认为1。
    # generator-num-goroutine = 1

    # 生成数据的缓存大小，即生成多少数据，调用一次写入writer的函数、向mxgate发一次请求，加载数据。
    # 默认为4，单位为MB。
    # generator-write-batch-size = 4
```

##### 2.2.3.2 file

从csv文件中读取数据并加载，选择generator="file"。

```toml
[generator]

  generator = "file"

  [generator.file]

    # 数据csv数据的绝对路径。可接收一个数组，即上传多个csv文件。
    generator-file-paths = []

    # 同[generator.telematics]，设置这两个参数非必需，
    # 但是妥当设置会帮助我们更好的生成DDL语句。如选定制DDL语句则将不起作用。
    generator-batch-size = 1
    generator-empty-value-ratio = 90
```

##### 2.2.3.3 nil

不生成、加载任何数据，选择generator="nil"。

```toml
[generator]

  generator = "nil"
```

#### 2.2.4 writer

##### 2.2.4.1 http(默认)

以http形式启动mxgate并加载数据。

```toml
[writer]

  writer = "http"

  [writer.http]

    # 发送http消息是否使用gzip压缩，默认不采用
    writer-use-gzip = false

    # 向mxgate发送数据的并发度。
    writer-parallel = 8

    # 指定mxgate二进制文件路径。
    # 默认为根据环境变量的PATH，直接使用"mxgate"启动。
    # writer-mxgate-path = ""

    # 打印的writer进度信息的格式， 支持 "list", "json"，默认为"list".
    # writer-progress-format = "list"

    # 打印的writer进度信息是否包括table大小，默认false，即不包括。
    # writer-progress-include-table-size = false

    # 打印的writer进度信息是否包括时区信息，默认false，即不包括。
    # writer-progress-with-timezone = false

    ## 高级调试，指定mxgate的interval参数，默认-1，即让mxbench自动指定。
    # writer-interval = -1

    ## 高级调试，指定mxgate的stream-prepared参数，默认-1，即让mxbench自动指定。
    # writer-stream-prepared = -1
```

##### 2.2.4.2 stdin

以stdin方式启动mxgate并加载数据。

```toml
[writer]

  writer = "stdin"

  # 指定mxgate二进制文件路径。
  # 默认为根据环境变量的PATH，直接使用"mxgate"启动。
  # writer-mxgate-path = ""

  # 打印的writer进度信息的格式， 支持 "list", "json"，默认为"list".
  # writer-progress-format = "list"

  # 打印的writer进度信息是否包括table大小，默认false，即不包括。
  # writer-progress-include-table-size = false

  # 打印的writer进度信息是否包括时区信息，默认false，即不包括。
  # writer-progress-with-timezone = false

  ## 高级调试，指定mxgate的interval参数，默认-1，即让mxbench自动指定。
  # writer-interval = -1

  ## 高级调试，指定mxgate的stream-prepared参数，默认-1，即让mxbench自动指定。
  # writer-stream-prepared = -1
```

##### 2.2.4.3 nil

不启动mxgate，不写入数据。

```toml
[writer]

  writer = "nil"
```

#### 2.2.5 benchmark

##### 2.2.5.1 telematics(默认)

```toml
[benchmark]

  benchmark = "telematics"

  [benchmark.telematics]

    # 将要顺序执行的telematics提供的query名称，
    # 现提供：
    # 1. "SINGLE_TAG_LATEST_QUERY" 
    # 2. "MULTI_TAG_LATEST_QUERY" 
    # 3. "SINGLE_TAG_DETAIL_QUERY" 
    # 共3个合法query名，分别为：
    # "SINGLE_TAG_LATEST_QUERY": 获得单车最近时间戳的各个指标值；
    # "MULTI_TAG_LATEST_QUERY": 随机选取10车，获得其最近时间戳的各个指标值；
    # "SINGLE_TAG_DETAIL_QUERY": 获得单车在一段时间内的各个指标的值。
    # 注：对于超宽表，指标数很多，可能DBMS不支持一次获取所有指标值，
    # 因此下面由参数可以调试获取的指标数以及"SINGLE_TAG_DETAIL_QUERY"的时间段取值。
    # 例如，输入[ "SINGLE_TAG_LATEST_QUERY", "MULTI_TAG_LATEST_QUERY", "SINGLE_TAG_DETAIL_QUERY" ] 就可以顺序执行上述三个query。
    # 在此基础上删减query名称便可不执行对应query。输入其他名称会被忽略。
    # 默认为空，即不执行任何预设query。
    benchmark-run-query-names = [ "SINGLE_TAG_DETAIL_QUERY" ]

    # 定制query，使用"," 分隔。
    # 例如, ["SELECT COUNT(*) from t1", "SELECT MAX(ts) from t1"]
    # 默认为空，即不执行任何定制query.
    benchmark-custom-queries = []

    # 组合式query的表达式，字符串，默认为空串。
    # 有自己的一套语法，相见“组合式query”板块。
    # benchmark-combination-queries = ""

    # 打印的benchmark进度信息的格式， 支持 "list", "json"，默认为"list".
    # benchmark-parallel = "list"

    # 跑query的并发度，可以输入多个，顺序对应并发度执行各个query，使用","分隔。
    # 例如，输入 [1, 8] 就可以先以并发度1跑各个query，再以并发度为8跑各个query。
    # 默认为空。
    benchmark-parallel = [8]

    # 每个query在每个并发度下跑的次数，根据这么多次跑的结果做延迟和TPS统计，默认为0。
    benchmark-run-times = 0

    # 每个query在每个并发度下跑的时间（秒）, 根据这段时间内query执行的结果做延迟和TPS统计。
    # 只在benchmark-run-times为0的情况下才生效。默认为60，即每个query在每个并发度下跑60秒。
    benchmark-runtime-in-second = "60"
```

##### 2.2.5.2 nil

如果不需要执行任何query，则将benchmark设为nil。

```toml
[benchmark]

  benchmark = "nil"
```

### 2.3 示例配置文件

#### 2.3.1 超宽稀疏表生成数据并运行混合负载

```toml
[database]
  db-database = "testdb2"
  db-master-port = 5432

[global]
  # 开启进度查看功能，默认即为true
  watch = true

  # 生成的DDL， GUCs最佳实践建议， Query等文件的存放目录
  workspace = "/home/mxadmin/mxbench/workspace"

  # 数据加载和查询是否同时进行
  simultaneous-loading-and-query = true

  table-name = "table2"

  # 设备数
  tag-num = 20000
  # 指标数据类型，支持 int4, int8, float4, float8 四种类型
  metrics-type = "float8"
  # 指标数目，如果指标数大于998，就把前997个作为简单列，
  # 其他的作为json存放在名为ext的列中
  total-metrics-count = 5000

  # 生成数据的起始时间戳，ts-end必须晚于ts-start，否则报错
  ts-start = "2022-04-19 00:00:00"
  ts-end = "2022-04-19 00:01:00"

[generator]
  generator = "telematics"

  [generator.telematics]
    # 每个设备每个时间点的指标，分几条数据上传，最终在DB被upsert成1个tuple
    generator-batch-size = 1
    # 延迟上传的数据生成比例（1～100），时间戳往前推1小时
    generator-disorder-ratio = 0
    # 生成数据的空值率（1～100）
    generator-empty-value-ratio = 90
    # 生成数据的随机度， 有OFF/S/M/L几档，默认关闭"OFF"
    generator-randomness = "OFF"

[writer]
  writer = "stdin"

[benchmark]
  benchmark = "telematics"

  [benchmark.telematics]
    # 数组，查询并发度
    benchmark-parallel = [64]
    # 提供的3个查询：单车最新值，10车最新值，单车明细
    benchmark-run-query-names = [ "SINGLE_TAG_LATEST_QUERY", "MULTI_TAG_LATEST_QUERY", "SINGLE_TAG_DETAIL_QUERY" ]
    # 每轮每条query的跑的次数或时间，让时间生效需要将次数设置为0，如下：
    benchmark-run-times = 0
    benchmark-runtime-in-second = "60"
```

#### 2.3.2 从外部读取DDL并外部读取csv文件加载，不跑查询

```toml
[database]
  db-database = "testdb3"
  db-master-port = 5432

[generator]
  # 从csv文件中读取数据
  generator = "file"

  [generator.file]
    generator-file-paths = ["/home/mxadmin/mxbench/data.csv"]


[global]

  table-name = "table3"

  watch = true
  workspace = "/home/mxadmin/mxbench/workspace"
  ddl-file-path = "/home/mxadmin/mxbench/ddl.sql"

[writer]
  writer = "stdin"

[benchmark]
  benchmark = "nil"
```

### 2.4 示例命令行

#### 2.4.1 超宽稀疏表生成数据并运行混合负载

使用示例配置文件1运行mxbench相当于使用以下命令行运行mxbench：

```bash
./bin/mxbench run \
  --db-database "testdb2" \
  --db-master-port 5432 \
  --db-user "mxadmin" \
  --workspace "/home/mxadmin/mxbench/workspace" \
  --simultaneous-loading-and-query \
  --table-name "table2" \
  --tag-num 25000 \
  --metrics-type "float8" \
  --total-metrics-count 5000 \
  --ts-start "2022-04-19 00:00:00" \
  --ts-end "2022-04-19 00:01:00" \
  --generator "telematics" \
  --generator-batch-size 1 \
  --generator-disorder-ratio 0 \
  --generator-empty-value-ratio 90 \
  --generator-randomness "OFF" \
  --writer "stdin" \
  --benchmark "telematics" \
  --benchmark-run-query-names "SINGLE_TAG_LATEST_QUERY" \
  --benchmark-run-query-names "MULTI_TAG_LATEST_QUERY" \
  --benchmark-run-query-names "SINGLE_TAG_DETAIL_QUERY" \
  --benchmark-parallel 64 \
  --benchmark-run-times 0 \
  --benchmark-runtime-in-second 60
```

#### 2.4.2 从外部读取DDL并外部读取csv文件加载，不跑查询

使用示例配置文件2运行mxbench相当于使用以下命令行运行mxbench：

```bash
./bin/mxbench run \
  --db-database "testdb3" \
  --db-master-port 5432 \
  --workspace "/home/mxadmin/mxbench/workspace" \
  --ddl-file-path "/home/mxadmin/mxbench/ddl.sql" \
  --table-name "table3" \
  --generator "file" \
  --generator-file-paths "/home/mxadmin/mxbench/data.csv" \
  --writer "stdin" \
  --benchmark "nil" 
```

### 2.5 FAQ

1. 只加载，不查询
 将benchmark设为nil;

2. 只查询，不加载
 将generator设为nil;

3. 加载和查询同时跑
 global设置中simultaneous-loading-and-query为true。

4. 想要生成并dump出csv数据文件
 global设置中dump为true, 生成的文件在workspace设置的目录下的<unix-timestamp>目录中。

5. 想要查看生成的ddl和query
 workspace设置的目录下的<unix-timestamp>目录中。

6. 想要跑定制DDL
 在global设置中的ddl-file-path中填写ddl文件的绝对路径。

7. 想要跑定制query
 在 telematics benchmark的 benchmark-custom-queries中填写定制query语句, 用""括起来。不支持随机参数。

8. 不想采用系统建议的GUCs， 保留现有GUCs运行mxbench:
 mxbench检测到现有系统和建议GUCs有不一致时，会在标准输出中做提示，并且询问是否需要重设GUCs并启动数据库。输入"N"，保留原有GUCs. mxbench这时还会再次确认是否继续运行mxbench。选择"Y", 继续运行。
 或者指定"set-skip-gucs"参数为true.

9. 对参数合法性有什么要求？
global 配置里: 
  - ts-end必须晚于ts-start；
  - table-name、schema-name 不为空;
  - tag-num必须大于0;
  - ts-step-in-second不为0。

## 3.理解进度信息和统计报告

### 3.1 进度信息

示例：

```bash
● Stdin Writer Report
  ● period start: 2022-04-29 10:08:11, end: 2022-04-29 10:08:16, period: 5.00s

  ● count written in total: 637025 rows/ 1500000 rows 42.12%, 637025 rows in this period

  ● size written in total: 160878718 bytes/ 360000003 bytes 44.23%, 160878718 bytes in this period

  ● size written to mxgate in total: 350075440 bytes, 350075440 bytes in this period

● Telematics Benchmark Report
  ● stats for query SINGLE_TAG_LATEST_QUERY with parallel 8: progress: 100%

  ● stats for query MULTI_TAG_LATEST_QUERY with parallel 8: progress: 43%
```

说明：总共由两部分组成，即writer和benchamrk的进度报告。

#### 3.1.1 writer

- period start, period end, period: 该统计窗口的起止时间和时间段；
- count written: 已经写入的数据行数和预计数据行数，以及二者的百分比。xx in this period：该统计窗口内写入的行数；
- size written: 已经写入的数据字节数和预计数据字节数，以及二者的百分比。xx in this period: 该统计窗口内写入的字节数；
- size written to mxgate: 把数据转化成文本后写入mxgate的字节数。xx in this period: 该统计窗口内写入mxgate的字节数。

#### 3.1.2 benchmark

某条query在某并发度parallel参数下的执行进度。query与数据加载同时进行时，query会在数据加载结束之前一直进行，因此可能会循环运行多轮。该进度报告只显示最近一轮的进度报告

### 3.2 统计报告

#### 3.2.1 writer

```bash
┌───────────────────────────────────────────────────────┐
│            Summary Report for Stdin Writer            │
├─────────────────────────────────┬─────────────────────┤
│ start time:                     │ 2022-04-27 13:29:01 │
├─────────────────────────────────┼─────────────────────┤
│ stop time:                      │ 2022-04-27 13:29:58 │
├─────────────────────────────────┼─────────────────────┤
│ size written to mxgate (bytes): │ 848333400           │
├─────────────────────────────────┼─────────────────────┤
│ lines inserted:                 │ 1500000             │
├─────────────────────────────────┼─────────────────────┤
│ compress ratio:                 │ 1.56 : 1            │
└─────────────────────────────────┴─────────────────────┘
```

- start time: 数据加载起始时间；
- end time: 数据加载终止时间；
- size written to mxgate (bytes): 向mxgate写入数据的字节数；
- lines inserted： 插入数据的条数. 由于upsert可能存在，这一数字可能会高于数据库中实际的数据条数。
- compress ratio: 压缩比，即向mxgate写入数据的大小和实际数据库中该表的大小的比值。

#### 3.2.2 benchmark

每条query，在每个parallel参数下都会产生一个报告，会实时打印出来。

```bash
┌─────────────────┬───────────────┐
│ Overall Duration│       29.942s │
│ Average Latency │       13.72ms │
│ P75 Latency     │       14.35ms │
│ P50 Latency     │       13.65ms │
│ P25 Latency     │       12.92ms │
│ TPS             │           582 │
└─────────────────┴───────────────┘
```

- Pxx 代表xx百分位数的延迟。例如P75是14.35972ms，说明执行query的次数中，有25%延迟高于它，75%低于它. P50即中位数。
- TPS： 每秒执行query的次数.

汇总报告：

```bash
┌───────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┐
│                                                Summary Report for Telematics Benchmark                                                │
├─────────────────────┬─────────────────────────────────────┬─────────────────────────────────────┬─────────────────────────────────────┤
│ Parallel\Query Name │ SINGLE_TAG_LATEST_QUERY             │ MULTI_TAG_LATEST_QUERY              │ SINGLE_TAG_DETAIL_QUERY             │
├─────────────────────┼─────────────────────────────────────┼─────────────────────────────────────┼─────────────────────────────────────┤
│ 8                   │ ┌─────────────────┬───────────────┐ │ ┌─────────────────┬───────────────┐ │ ┌─────────────────┬───────────────┐ │
│                     │ │ Overall Duration│        30.00s │ │ │ Overall Duration│       36.401s │ │ │ Overall Duration│        29.94s │ │
│                     │ │ Average Latency │       23.81ms │ │ │ Average Latency │         7.27s │ │ │ Average Latency │       13.72ms │ │
│                     │ │ P75 Latency     │       24.91ms │ │ │ P75 Latency     │         8.13s │ │ │ P75 Latency     │       14.31ms │ │
│                     │ │ P50 Latency     │       23.41ms │ │ │ P50 Latency     │         7.16s │ │ │ P50 Latency     │       13.65ms │ │
│                     │ │ P25 Latency     │       20.89ms │ │ │ P25 Latency     │         6.64s │ │ │ P25 Latency     │       12.92ms │ │
│                     │ │ TPS             │           335 │ │ │ TPS             │             1 │ │ │ TPS             │           582 │ │
│                     │ └─────────────────┴───────────────┘ │ └─────────────────┴───────────────┘ │ └─────────────────┴───────────────┘ │
│                     │ progress: 100%                      │ progress: 100%                      │ progress: 100%                      │
└─────────────────────┴─────────────────────────────────────┴─────────────────────────────────────┴─────────────────────────────────────┘
```

- 每行代表某并发度下各query的执行结果。
- 每列代表每个query在各个并发度下的执行结果。
- 如果query集执行了多轮（混合负载的情况下，数据加载未结束，query便会一直执行），则仅展示最后一轮的结果。
- 如果因为query执行错误或者用户中断执行，进度条会显示当前进度，统计信息是根据已经执行query的做出统计。

## 4.多数据类型与特征功能

### 4.1 背景
> 目前自定义类型和特征仅支持指标列，即非VIN和TS列。

#### 4.1.1 之前我们有
1. 只支持信号表中采集一种指标类型；is-ext
2. 支持的类型：int4， int8， float4， float8；任选一种。

#### 4.1.2 现在我们有……
1. 支持信号表中采集多种不同类型的指标；
2. 除了int4， int8， float4， float8 这四种之外，还有text和varchar等类型。
3. int4， int8， float4， float8 这四种还可以指定取值范围等数据特征。
4. varchar类型还提供了特殊的类型以更好的适应车联网场景的需求：
  plate_template: 即生成车牌号类型特征的varchar列，如：`粤BDM7709`。
  vin_template: 即生成车架号类型特征的varchar列，如：`1G1JC124627237595`。

### 4.2 限制
1. 第1列必须是ts列，列名不限，类型必须是timestamp或timestamptz；
2. 第2列必须是vin列，列名不限，类型必须是text, varchar,  int8中的其中一种。

### 4.3 通过DDL
DDL文件中，创建表之后，通过为列添加comment来表达该列的用途；
```SQL
CREATE EXTENSION IF NOT EXISTS matrixts;
ALTER EXTENSION matrixts UPDATE;
CREATE SCHEMA IF NOT EXISTS "public";
CREATE TABLE "public"."table1" (
    tsss timestamp
,   vinnn bigint
,   lltt varchar(32)
,   c_arb float8
,   exttt json
)
USING mars2 WITH ( compress_threshold='1000', chunk_size='32' )
DISTRIBUTED BY (vinnn)
PARTITION BY RANGE(tsss) (
        START ('2022-04-18 09:00:00')
        END ('2022-04-19 09:00:06')
        EVERY ('10800 second'),
        DEFAULT PARTITION default_prt
);

CREATE INDEX IF NOT EXISTS "idx_table1" ON "public"."table1"
USING mars2_btree(
        vinnn
      , tsss)
  
COMMENT ON COLUMN table1.lltt is '{"name": "plate_template"}';
COMMENT ON COLUMN table1.c_arb is '{"min": 3.5, "max": 5}';
COMMENT ON COLUMN table1.exttt is '{"is-ext": true, "columns-descriptions": [{"type": "float8", "count": 1, "comment": {"min": 3, "max": 4}},{"type": "float4", "count": 3, "comment": {"min": 2, "max": 3}}]}';
```
示例配置如上，comment全部为JSON格式字符串，说明：
1. 前两列，见“限制”小节；
2. 除了前2列外，其他都被视为指标列；
3. lltt是varchar类型，且comment中标出：name=license_template， 则会为lltt生成车牌号类型的数据；
4. c_arb是个float8类型的指标，根据comment，会生成3.5 ~ 5范围的随机数据；
5. table1.exttt 是个json类型的列，且被标注：is-ext=true， 它就是被标注成了扩展指标列，可能包含多个简单指标；哪些简单指标呢？有两种方式：
  1. 如果未指定columns-descriptions，则会读取GlobalConfig中的total-metrics-count, 和 metrics-type两个参数。total-metrics-count的配置如果小于（简单指标列总数+2），报错，因为假定这个扩展列至少被拍进了2个指标；这个例子里，如果没有columns-descriptions(注释掉的那一行)，简单指标列就c_arb，为1，所以total-metrics-count大于等于3即可。如果total-metrics-count配成5，则这个扩展列储存4个指标；
  2. 指定了columns-descriptions， 且必须是合法json array字符串，则GlobalConfig中的total-metrics-count, 和 metrics-type两个参数会失效，扩展列里有什么指标，有多少指标全部看这个columns-descriptions。我们来看看这个json字符串的示例：
```json
[
{"type": "float8", "count": 1, "comment": {"min":  3, "max": 4}},  
{"type": "float4", "count": 3, "comment": {"min":  2, "max": 3}}, 
]
```
所以，这个扩展列中有1个float8类型的指标，3个float4类型的指标，且有取值范围的限制。

### 4.4 通过配置文件
复用上面的扩展列的columns-description语法, global 模块下的 metrics-descriptions 接受一个字符串类型的参数。
```toml
[global]
  table-name = "table1"
  metrics-descriptions =  """[{"type": "float8", "count": 1000, "comment": {"min": 3, "max": 4}},{"type": "float4", "count": 3, "comment": {"min": 2, "max": 3}}]"""
```
看看这个字符串：
```
[
{"type": "float8", "count": 1000, "comment": {"min": 3, "max": 4}},
{"type": "float4", "count": 3, "comment": {"min": 2, "max": 3}}
]
```
就说明，用户有1000个float8类型的指标，3个float4类型的指标。总指标1003个，如果全部作为简单指标，即一个指标一列，就有1005列超过了我们的限制（1000列），就会把6个指标作为拍到json类型的扩展列里面。这样就只有997个简单指标 + 2个固定指标（ts， vin）+ 1个扩展列，1000列，正好是我们的限制。
这个例子里面997个float8类型的列会作为简单列，3个float8 + 3个float4拍成json类型的扩展列。

> 如果metrics-descriptions没有指定，就看GlobalConfig里面的total-metrics-count和metrics-type，兼容老的用法。

## 5.组合式query
### 5.1 背景
直接输入定制化query有时候无法满足我们的需求，例如有些SELECT、WHERE语句中的一些数值需要根据本次生成数据的特征来决定。

### 5.2 配置详解
### 5.2.1 概览
```json
    {
    // 该条query的名称
    "name": "QUERY_NAME",
    // SELECT后的语句
    "projections": {"use-raw-expression": true, "expression": "*"},
    // SELECT FROM 哪个relation
    "from": {"use-relation-identifier": true, "relation-identifier": "sub-relation-identifier"},
    // 设备断言
    "device-predicate": {"count": 2, "is-random": ture},
    // 时间戳断言
    "ts-predicate": {"start": "2022-05-05 01:04:10", "end": "2022-05-05 01:04:10"},
    // 指标过滤断言
    "metrics-predicate": {"use-raw-expression": true, "expression": "m1>=37.5"},
    // GROUP BY语句
    "group-by": {"use-raw-expression": true, "expression": "device_column_name,ts"},
    // ORDER BY 语句
     "order-by": {"user-raw-expression": true, "": "s desc"},
     // LIMIT 语句
     "limit": 3
    },
```

### 5.2.2 详解

#### 5.2.2.1  "name"
该条query的名称，最后显示在query汇总报告里。

#### 5.2.2.2  "projections"
不允许缺省。SELECT 后面的表达式，字段投影。接受一个JSON类型的配置。

使用raw-expression:
```json
{"use-raw-expression": true, "expression": "*"}
```

#### 5.2.2.3  "from"
FROM 后面的表达式。

缺省：即直接从从Global Config里面配置的table-name，等价于：
```json
{
 "use-relation-identifier": true, 
 "relation-identifier": "table-name-in-global-config",
 }
```

直接使用relation的名字：
```json
 {
 "use-relation-identifier": true, 
 "relation-identifier": "device_signal_mars2",
 }
```

使用另一个relation statement：
```json
 { 
   "relation-statement":
    {
    "projections": {"use-raw-expression": true, "expression": "device_column_name,max(m1) as mp, min(m2), count(m1), avg(m3)"},
    "device-predicate": {"count": 17, "is-random": true},
    "ts-predicate": {"start": "2022-05-05 00:00:00", "end": "2022-05-06 00:00:00"},
    "group-by": {"use-raw-expression": true, "expression": "device_column_name"}
    }
 }
```

#### 5.2.2.4  "device-predicate"
有关设备号的断言，接受一个JSON类型的配置。

缺省:没有关于设备号的断言，即全设备。

随机选取n（n是正整数，处于设备数范围内）个设备:
例如n=2， 以下配置生成：`WHERE <device_column_name> IN (<random_device_id1>, <random_device_id2>)`。

```json
  {
  "count": 2, 
  "is-random": false
  }
```

若n=1，则会解释为等值查询，以下配置生成：`WHERE <device_column_name>=<random_device_id>`。

```json
  {
  "count": 1, 
  "is-random": false
  }
```

使用raw-expression: "expression" 后的字符串直接出现在WHERE后。和其他predicates是"AND"的关系。
```json
  {
  "use-raw-expression": true, 
  "expression": "device_column_name IN (1234, 4321)"
  }
```

#### 5.2.2.5 "ts-predicate"
有关时间戳的断言，接受一个JSON类型的配置。

缺省: 没有关于设备号的断言，即全设备。

随机选取1个时间点（在生成数据的ts范围内）:
```json
  {
  "is-random": true, 
  "duration": 3600  
  }
```

设置起止时间: 假设时间戳字段名为ts，则会生成`WHERE ts >= '2022-07-15 18:07:00' AND ts <= '2022-07-15 18:31:17' 的断言`。
```json
 {
  "start": "2022-07-15 18:07:00", 
  "end": "2022-07-15 18:31:17",
  }
```

等值查询: `WHERE ts = '2022-07-15 18:07:00'`
```json
 {
  "start": "2022-07-15 18:07:00", 
  "end": "2022-07-15 18:07:00",
  }
```

开区间查询: `WHERE ts >= '2022-07-15 18:07:00' AND ts < '2022-07-15 18:31:17'`:
```json
 {
  "start": "2022-07-15 18:07:00", 
  "end": "2022-07-15 18:07:00",
  "end-exclusive": true
  }
```

为时间戳字段设置alias: `WHERE tttt >= '2022-07-15 18:07:00' AND tttt < '2022-07-15 18:31:17'`:
```json
 {
  "has-alias": true,
  "alias": "tttt",
  "start": "2022-07-15 18:07:00", 
  "end": "2022-07-15 18:07:00",
  "end-exclusive": true
  }
```

使用raw-expression:"expression" 后的字符串直接出现在WHERE后。和其predicates是"AND"的关系。
```json
  {
  "use-raw-expression": true, 
  "expression": "ts='2022-07-16 10:31:17'"
  }
```

#### 5.2.2.6 "metrics-predicate"
有关指标过滤的断言，接受一个JSON类型的配置。

缺省: 不添加指标过滤。

使用raw-expression: "expression" 后的字符串直接出现在WHERE后。和其他predicates是"AND"的关系。
```json
  {
  "use-raw-expression": true, 
  "expression": "m1>=37.5"
  }
```

#### 5.2.2.7 "group-by"
GROUP BY语句，接受一个JSON类型的配置。

缺省: 不添加GROUP BY语句。

使用raw-expression: 
```json
  {
  "use-raw-expression": true, 
  "expression": "device_column_name,ts"
  }
```
#### 5.2.2.8 "order-by"
ORDER BY语句，接受一个JSON类型的配置。

缺省: 不添加ORDER BY语句。

使用raw-expression:
```json
{
"user-raw-expression": true, 
"expression": "s desc"
}
```

#### 5.2.2.8 "limit"
接受一个正整数。
缺省或设为小于等于0的数则表达不加LIMIT表达式。
