package metadata

import (
	"fmt"
	"math"
	"math/rand"
	"strings"
	"time"

	"github.com/lib/pq"

	"github.com/ymatrix-data/mxbench/internal/util"
	"github.com/ymatrix-data/mxbench/internal/util/mxerror"
)

const (
	OutOrderDuration = 24 * time.Hour

	// TODO
	sizeOfPartition          = 1024 * 1024 * 1024 * 1024
	minDurationPerPartition  = time.Hour * 3
	numberOfSecondsInOneHour = 3600
)

const (
	_CREATE_EXTENSION_SQL = `
CREATE EXTENSION IF NOT EXISTS matrixts;
ALTER EXTENSION matrixts UPDATE;`
	_CREATE_SCHEMA_SQL = `
CREATE SCHEMA IF NOT EXISTS %s;`
	_CREATE_TABLE_SQL_FMT = `
CREATE TABLE %s (
%s
)
USING %s
DISTRIBUTED BY (%s)
PARTITION BY RANGE(ts) (
	START ('%s')
	END ('%s')
	EVERY ('%d second'),
	DEFAULT PARTITION default_prt
);
`
	_CREATE_TABLE_WITH_OPTIONS_SQL_FMT = `
CREATE TABLE %s (
%s
)
USING %s WITH ( %s )
DISTRIBUTED BY (%s)
PARTITION BY RANGE(ts) (
	START ('%s')
	END ('%s')
	EVERY ('%d second'),
	DEFAULT PARTITION default_prt
);
`
	_SELECT_TABLE_SIZE_SQL = `
WITH RECURSIVE pg_inherit(inhrelid, inhparent) AS (
  SELECT
    inhrelid,
    inhparent
  FROM
    pg_inherits
  UNION
  SELECT
    child.inhrelid,
    parent.inhparent
  FROM
    pg_inherit child,
    pg_inherits parent
  WHERE
    child.inhparent = parent.inhrelid
),
pg_inherit_short AS (
  SELECT
    *
  FROM
    pg_inherit
  WHERE
    inhparent NOT IN (
      SELECT
        inhrelid
      FROM
        pg_inherit
    )
)
SELECT
  total_bytes
FROM
  (
    SELECT
      c.oid,
      sum(pg_total_relation_size(c.oid)) OVER (PARTITION BY parent) AS total_bytes,
      parent,
      relname
    FROM
      (
        SELECT
          pg_class.oid,
          relnamespace,
          relname,
          COALESCE(inhparent, pg_class.oid) parent
        FROM
          pg_class
          LEFT JOIN pg_inherit_short ON inhrelid = oid
        WHERE
          relkind IN ('r', 'p')
      ) c
      LEFT JOIN pg_namespace n ON n.oid = c.relnamespace
    WHERE
      n.nspname = '%s'
  ) a
WHERE
  oid = parent AND relname='%s';
`
)

type Metadata struct {
	GUCs  GUCs
	Table *Table
	Cfg   *Config
}

func New(cfg *Config) (*Metadata, error) {
	if err := cfg.validate(); err != nil {
		return nil, err
	}

	metadata := &Metadata{
		Cfg:  cfg,
		GUCs: NewGUCs(cfg),
	}

	if cfg.IsDDLFromFile {
		var err error
		metadata.Table, err = NewTableFromDB(cfg)
		if err != nil {
			err = mxerror.CommonErrorf("error occurs when read table from db: %v", err)
		}
		return metadata, err
	}

	// TODO: support multi-storage-type
	var err error
	metadata.Table, err = NewMars2Table(cfg)

	return metadata, err
}

func (meta *Metadata) GetDDL() string {
	return _CREATE_EXTENSION_SQL +
		meta.createSchemaSQL() +
		meta.createTableSQL() +
		meta.createIndexSQL()
}

func (meta *Metadata) GetTableSizeSQL() string {
	return fmt.Sprintf(_SELECT_TABLE_SIZE_SQL, meta.Table.schemaName, meta.Table.name)
}

func (meta *Metadata) createSchemaSQL() string {
	return fmt.Sprintf(_CREATE_SCHEMA_SQL, pq.QuoteIdentifier(meta.Cfg.SchemaName))
}

func (meta *Metadata) createTableSQL() string {
	start := meta.Cfg.StartAt
	end := meta.Cfg.EndAt

	if !start.Before(end) {
		return ""
	}

	partitionIntevalInSecond := meta.Cfg.PartitionIntervalInHour * numberOfSecondsInOneHour
	if meta.Cfg.PartitionIntervalInHour <= 0 {
		// TODO: Provide a better partition duration according to data size
		amountSize := (float64(100-meta.Cfg.EmptyValueRatio) / 100) *
			float64(meta.Cfg.TagNum*
				meta.Cfg.TotalMetricsCount*
				Size(meta.Cfg.MetricsType)*
				(int64(end.Sub(start))/
					(int64(meta.Cfg.TimestampStepInSecond)*int64(time.Second))))

		partitionNum := int64(math.Ceil(amountSize / float64(sizeOfPartition)))
		if partitionNum <= 0 {
			partitionNum = 1
		}
		partitionIntevalInSecond = int64(end.Sub(start) / (time.Duration(partitionNum) * time.Second))
	}

	if partitionIntevalInSecond < int64(minDurationPerPartition.Seconds()) {
		partitionIntevalInSecond = int64(minDurationPerPartition.Seconds())
	}

	tableIdentifier := meta.Table.Identifier()

	if len(meta.Table.Options) > 0 {
		return fmt.Sprintf(
			_CREATE_TABLE_WITH_OPTIONS_SQL_FMT,
			tableIdentifier,
			meta.Table.Columns.ToSQLStr(),
			meta.Table.Storage,
			meta.Table.Options.ToSQLStr(),
			meta.Table.DistKey,
			meta.Cfg.StartAt.Add(-OutOrderDuration).Format(util.TIME_FMT),
			meta.Cfg.EndAt.Format(util.TIME_FMT),
			partitionIntevalInSecond,
		)
	}

	return fmt.Sprintf(
		_CREATE_TABLE_SQL_FMT,
		tableIdentifier,
		meta.Table.Columns.ToSQLStr(),
		meta.Table.Storage,
		meta.Table.DistKey,
		meta.Cfg.StartAt.Format(util.TIME_FMT),
		meta.Cfg.EndAt.Format(util.TIME_FMT),
		partitionIntevalInSecond,
	)
}

func (meta *Metadata) createIndexSQL() string {
	createIndexSQL := ""
	for _, index := range meta.Table.Indexes {
		createIndexSQL += index.GetCreateIndexSQLStr()
	}
	return createIndexSQL
}

type SingleVinGenerator func() string
type MultiVinsGenerator func() string
type DurationGenerator func() (string, string)

func (meta *Metadata) GetSingleVinGenerator() SingleVinGenerator {
	return func() string {
		return meta.GetRandomVinsGenerator(1)()
	}
}

func (meta *Metadata) GetRandomVinsGenerator(num int) func() string {
	return func() string {
		if len(meta.Table.VinValues) == 0 {
			return "''"
		}
		quotedSelectedTags := make([]string, 0, num)
		for i := 0; i < num; i++ {
			vinIdx := rand.Int63n(int64(len(meta.Table.VinValues)))
			quotedSelectedTags = append(quotedSelectedTags, pq.QuoteLiteral(meta.Table.VinValues[vinIdx]))
		}
		return strings.Join(quotedSelectedTags, ", ")
	}
}

func (meta *Metadata) GetRandomStartEndTSArgGenerator(duration time.Duration) DurationGenerator {
	return func() (string, string) {
		randomStartTime := meta.Cfg.StartAt
		endTime := meta.Cfg.EndAt
		if endTime.Sub(randomStartTime) > duration {
			randomStartTime = randomStartTime.Add(time.Duration(rand.Int63n(int64(endTime.Add(-duration).Sub(randomStartTime)))))
			endTime = randomStartTime.Add(duration)
		}
		return pq.QuoteLiteral(randomStartTime.Format(util.TIME_FMT)), pq.QuoteLiteral(endTime.Format(util.TIME_FMT))
	}
}

func (meta *Metadata) GetFixedStartEndTSArgGenerator(startTime, endTime string) DurationGenerator {
	return func() (string, string) {
		return pq.QuoteLiteral(startTime), pq.QuoteLiteral(endTime)
	}
}

func (meta *Metadata) ToJSONSelectStr(jsonColumnsDescs MetricsDescriptions, tableAlias string, jsonMetricsNum int64) string {
	if jsonMetricsNum > meta.Table.JSONMetricsCount {
		jsonMetricsNum = meta.Table.JSONMetricsCount
	}
	jsonSelectStrSlice := make([]string, 0, jsonMetricsNum)
	deli := "."
	if tableAlias == "" {
		deli = ""
	}
	if jsonColumnsDescs == nil {
		for i := int64(0); i < jsonMetricsNum; i++ {
			jsonSelectStrSlice = append(jsonSelectStrSlice, fmt.Sprintf("%s%sk%d", tableAlias, deli, i))
		}
		return strings.Join(jsonSelectStrSlice, "\n  , ")

	}

	var count int64
	for cdI, columnsDesc := range jsonColumnsDescs {
		if count >= jsonMetricsNum {
			break
		}
		if count+columnsDesc.Count <= jsonMetricsNum {
			for cdc := int64(0); cdc < columnsDesc.Count; cdc++ {
				jsonSelectStrSlice = append(jsonSelectStrSlice, fmt.Sprintf("%s%sk%d_%s_%d", tableAlias, deli, cdI, columnsDesc.MetricsType, cdc))
				count++
			}
			continue
		}
		for cdc := int64(0); cdc < jsonMetricsNum-count; cdc++ {
			jsonSelectStrSlice = append(jsonSelectStrSlice, fmt.Sprintf("%s%sk%d_%s_%d", tableAlias, deli, cdI, columnsDesc.MetricsType, cdc))
			count++
		}
	}
	return strings.Join(jsonSelectStrSlice, "\n  , ")
}

func (meta *Metadata) ToJSONColStr(jsonColumnsDescs MetricsDescriptions, jsonMetricsNum int64) string {
	if jsonMetricsNum > meta.Table.JSONMetricsCount {
		jsonMetricsNum = meta.Table.JSONMetricsCount
	}
	jsonColStrSlice := make([]string, 0, jsonMetricsNum)
	if jsonColumnsDescs == nil {
		for i := int64(0); i < jsonMetricsNum; i++ {
			jsonColStrSlice = append(jsonColStrSlice, fmt.Sprintf("%s %s", pq.QuoteIdentifier(fmt.Sprintf("k%d", i)), meta.Cfg.MetricsType))
		}
		return strings.Join(jsonColStrSlice, "\n  , ")
	}
	var count int64
	for cdI, columnsDesc := range jsonColumnsDescs {
		if count >= jsonMetricsNum {
			break
		}
		if count+columnsDesc.Count <= jsonMetricsNum {
			for cdc := int64(0); cdc < columnsDesc.Count; cdc++ {
				jsonColStrSlice = append(jsonColStrSlice, fmt.Sprintf("%s %s", pq.QuoteIdentifier(
					fmt.Sprintf("k%d_%s_%d", cdI, columnsDesc.MetricsType, cdc)), columnsDesc.MetricsType))
				count++
			}
			continue
		}
		for cdc := int64(0); cdc < jsonMetricsNum-count; cdc++ {
			jsonColStrSlice = append(jsonColStrSlice, fmt.Sprintf("%s %s", pq.QuoteIdentifier(
				fmt.Sprintf("k%d_%s_%d", cdI, columnsDesc.MetricsType, cdc)), columnsDesc.MetricsType))
			count++
		}
	}
	return strings.Join(jsonColStrSlice, "\n  , ")
}
