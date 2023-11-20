package metadata

import (
	"fmt"
	"strings"

	"github.com/lib/pq"

	"github.com/ymatrix-data/mxbench/internal/util"
	"github.com/ymatrix-data/mxbench/internal/util/mxerror"
	"github.com/ymatrix-data/mxbench/pkg/mxmock"
)

const (
	// TODO: make it configurable
	MAX_SIMPLE_COLUMN_NUM  = 1000
	NON_METRICS_COLUMN_NUM = 2 // ts, vin
	EXT_COLUMN_NUM         = 1 // only 1 ext column, json/jsonb
)

type StorageType = string

const (
	StorageMars2 StorageType = "mars2"
	StorageMars3 StorageType = "mars3"
	StorageHeap  StorageType = "heap"
)

type Option struct {
	Name  string
	Value interface{}
}

type Options []*Option

func (opts Options) ToSQLStr() string {
	vars := make([]string, 0, len(opts))
	for _, opt := range opts {
		vars = append(vars, fmt.Sprintf(
			"%s=%s",
			opt.Name,
			pq.QuoteLiteral(fmt.Sprintf("%v", opt.Value)),
		))
	}
	return strings.Join(vars, ", ")
}

type MetricsDescription struct {
	MetricsType MetricsType `json:"type"`
	Count       int64       `json:"count"`
	Spec        ColumnSpec  `json:"comment"`
}

type MetricsDescriptions []*MetricsDescription

func (mds MetricsDescriptions) getTotalMetricsCount() int64 {
	var count int64
	for _, cd := range mds {
		count += cd.Count
	}
	return count
}

type Table struct {
	schemaName               string
	name                     string
	Columns                  Columns
	ColumnSpecs              ColumnSpecs
	TotalMetricsCount        int64
	JSONMetricsCount         int64
	JSONMetricsCandidateType MetricsType

	ColumnNameTS, ColumnNameVIN, ColumnNameExt string
	ColumnTypeTS, ColumnTypeVIN, ColumnTypeExt ColumnType

	ExtColumn       *mxmock.Column
	ColumnsDescsExt MetricsDescriptions
	VinValues       []string

	// if it is from DDL, then the fields below are disabled
	// TODO: may also do initialization in the future
	DistKey    string
	OrderByKey []string
	Storage    StorageType
	Options    Options
	Indexes    Indexes
}

func (t *Table) Identifier() string {
	return fmt.Sprintf("%s.%s", pq.QuoteIdentifier(t.schemaName), pq.QuoteIdentifier(t.name))
}

func (t *Table) SingleRowMetricsSize() int64 {
	var singleRowMetricsSize int64
	for i := NON_METRICS_COLUMN_NUM; i < len(t.Columns); i++ {
		singleRowMetricsSize += Size(t.Columns[i].TypeName)
	}
	return singleRowMetricsSize
}

// NewMarsTable creates a mars2 or mars3 table with an index according to config.
func NewMarsTable(cfg *Config, st StorageType) (*Table, error) {
	var err error
	tb, err := NewTable(cfg, st)
	if err != nil {
		return nil, err
	}

	// Try to initialize the table's columns
	// First of all, try to read and parse metrics-descriptions as a JSON array,
	// which encodes types and columns, as well as specifications of configured metrics
	var metricsDescs, simpleMetricsDescs MetricsDescriptions
	if cfg.MetricsDescriptions != "" {
		metricsDescs, err = parseColumnsDescriptions(cfg.MetricsDescriptions)
		// If the metrics-descriptions is ill-formatted, return the error
		if err != nil {
			return nil, err
		}
		// table's TotalMetricsCount needs to be reset according to the parsing result
		tb.TotalMetricsCount = metricsDescs.getTotalMetricsCount()
		simpleMetricsDescs = metricsDescs
	}

	simpleMetricsCount := tb.TotalMetricsCount
	columnNum := simpleMetricsCount + NON_METRICS_COLUMN_NUM

	// Check the total metrics, and possibly separate a subset of them into the ext column
	var extMetricsDescs MetricsDescriptions
	if columnNum > MAX_SIMPLE_COLUMN_NUM {
		// compact a subset of metrics into the ext column
		simpleMetricsCount = MAX_SIMPLE_COLUMN_NUM - NON_METRICS_COLUMN_NUM - EXT_COLUMN_NUM
		tb.JSONMetricsCount = tb.TotalMetricsCount - simpleMetricsCount
		columnNum = MAX_SIMPLE_COLUMN_NUM
		// reassemble columnDescs for both simple columns and ext column
		simpleMetricsDescs, extMetricsDescs = separateColumnDescs(metricsDescs, simpleMetricsCount)
	}

	columns := make(Columns, NON_METRICS_COLUMN_NUM, columnNum)
	var encoding string
	if encoding, err = genEncoding(st); err != nil {
		return nil, err
	}
	columns[TSColumnIndex] = NewColumn(tb.ColumnNameTS, tb.ColumnTypeTS).WithEncoding(encoding)
	columns[VINColumnIndex] = NewColumn(tb.ColumnNameVIN, tb.ColumnTypeVIN).WithEncoding(encoding)

	columnSpecs := make(ColumnSpecs, NON_METRICS_COLUMN_NUM, columnNum)

	if simpleMetricsDescs != nil {
		c, cs := NewColumnsFromColumnsDescriptions(simpleMetricsDescs)
		columns = append(columns, c...)
		columnSpecs = append(columnSpecs, cs...)
	} else {
		for i := int64(0); i < simpleMetricsCount; i++ {
			columns = append(
				columns,
				NewColumn(fmt.Sprintf("c%d", i), cfg.MetricsType),
			)
			columnSpecs = append(columnSpecs, nil)
		}
	}
	var extCol *mxmock.Column
	var extColSpec *ColumnSpec
	if extMetricsDescs != nil {
		extCol, extColSpec, err = NewExtColumnFromColumnsDescriptions(tb.ColumnNameExt, tb.ColumnTypeExt, extMetricsDescs)
		if err != nil {
			return nil, err
		}
		columns = append(columns, extCol)
		columnSpecs = append(columnSpecs, extColSpec)
	} else if tb.JSONMetricsCount > 0 {
		extCol = NewColumn(ColumnNameExt, MetricsTypeJSON)
		columns = append(columns, extCol)
		columnSpecs = append(columnSpecs, nil)
	}

	tb.Columns = columns
	tb.ColumnSpecs = columnSpecs
	tb.ColumnsDescsExt = extMetricsDescs

	// TODO(BP): decide TimeBucketInSecond according to ts-step etc.
	// time_bucket may be deprecated in mars2_btree

	tb.Indexes, err = genMarsIndex(tb, st, cfg.HasUniqueConstraints)
	if err != nil {
		return nil, err
	}

	return tb, nil
}

func NewTable(cfg *Config, st StorageType) (*Table, error) {
	var orderKey []string
	var ops Options

	switch st {
	case StorageMars2:
		orderKey = nil
		ops = Options{
			{
				Name:  "compress_threshold",
				Value: 1000,
			},
			{
				Name:  "chunk_size",
				Value: 32,
			},
		}

	case StorageMars3:
		orderKey = []string{ColumnNameVIN, ColumnNameTS}
		ops = Options{
			{
				Name:  "compresstype",
				Value: "lz4",
			},
			{
				Name:  "mars3options",
				Value: "prefer_load_mode=bulk",
			},
		}
		if cfg.HasUniqueConstraints {
			ops = append(ops, &Option{
				Name:  "uniquemode",
				Value: "true",
			})
		}
	default:
		return nil, mxerror.CommonErrorf("unsupport storage type: %s", st)
	}

	tb := &Table{
		Storage: st,
		// Inherit basic information from config
		schemaName:        cfg.SchemaName,
		name:              cfg.TableName,
		TotalMetricsCount: cfg.TotalMetricsCount,
		// Use default name and type for special columns:
		// timestamp, vin, and ext column(may not be used, though)
		ColumnNameTS:  ColumnNameTS,
		ColumnNameVIN: ColumnNameVIN,
		ColumnNameExt: ColumnNameExt,
		ColumnTypeTS:  ColumnTypeTimestamp,
		ColumnTypeVIN: ColumnTypeText,
		ColumnTypeExt: ColumnTypeJSON,

		JSONMetricsCandidateType: cfg.MetricsType,
		// Set distribution key is as default: vin
		DistKey:    ColumnNameVIN,
		OrderByKey: orderKey,
		Options:    ops,
	}
	return tb, nil
}

func genEncoding(st StorageType) (string, error) {
	switch st {
	case StorageMars2:
		return "minmax", nil
	case StorageMars3:
		return "", nil
	default:
		return "", mxerror.CommonErrorf("unsupport storage type: %s", st)
	}
}

func genMarsIndex(tb *Table, st StorageType, isUnique bool) (Indexes, error) {
	switch st {
	case StorageMars2:
		return Indexes{NewMars2BTree(tb, 60, isUnique)}, nil
	case StorageMars3:
		return Indexes{NewMars3BTree(tb)}, nil
	default:
		return nil, mxerror.CommonErrorf("unsupport storage type: %s", st)
	}
}

func NewHeapTable(cfg *Config) (*Table, error) {
	// TODO
	return nil, nil
}

func NewTableFromDB(cfg *Config) (*Table, error) {
	// If table exists, read columns from database
	conn, err := util.CreateDBConnection(cfg.DB)
	if err != nil {
		return nil, err
	}
	defer func() {
		_ = conn.Close()
	}()

	columns, err := mxmock.NewColumnsFromDB(conn, cfg.SchemaName, cfg.TableName)
	if err != nil {
		return nil, err
	}

	table, err := NewTableFromColumns(cfg.SchemaName, cfg.TableName, columns)
	if err != nil {
		return nil, err
	}
	// Check and reset TotalMetricsCount
	if table.ExtColumn != nil && table.JSONMetricsCount == 0 {
		table.JSONMetricsCount = cfg.TotalMetricsCount - table.TotalMetricsCount
		if table.JSONMetricsCount <= 1 {
			return nil, mxerror.CommonErrorf("the ext column is to accommodate one or fewer metrics according to total-metrics-count: %d", cfg.TotalMetricsCount)
		}
		table.TotalMetricsCount = cfg.TotalMetricsCount
	}
	table.JSONMetricsCandidateType = cfg.MetricsType

	return table, nil
}

func NewTableFromColumns(schemaName, tableName string, columns Columns) (*Table, error) {
	// check the columns
	// the count of the should be greater than 2, which are left for timestamp and vin
	if len(columns) <= NON_METRICS_COLUMN_NUM {
		return nil, mxerror.CommonErrorf("the count of columns %d is not bigger than %d", len(columns), NON_METRICS_COLUMN_NUM)
	}

	// check the leading 2 columns of columns:
	// They must be： the 1st for timestamp, and the 2nd for vin
	firstCol, secondCol := columns[TSColumnIndex], columns[VINColumnIndex]
	if !isSupportedTSType(firstCol.TypeName) {
		return nil, mxerror.CommonErrorf("an unsupported column is designated as \"timestamp\" column: %s of %s", firstCol.Name, firstCol.TypeName)
	}
	if !isSupportedVinType(secondCol.TypeName) {
		return nil, mxerror.CommonErrorf("an unsupported column is designated as \"vin\" column: %s of %s", firstCol.Name, firstCol.TypeName)
	}
	table := &Table{
		schemaName:        schemaName,
		name:              tableName,
		Columns:           columns,
		ColumnSpecs:       make(ColumnSpecs, len(columns)),
		TotalMetricsCount: int64(len(columns) - NON_METRICS_COLUMN_NUM),
		ColumnNameTS:      firstCol.Name,
		ColumnNameVIN:     secondCol.Name,
		ColumnTypeTS:      firstCol.TypeName,
		ColumnTypeVIN:     secondCol.Name,
	}
	// traverse the columns, and check:
	var extCol *mxmock.Column
	var extColSpec *ColumnSpec
	var err error
	// Only the 'ts' column dosen't using ColumnSpec
	for i := 1; i < len(columns); i++ {
		col := columns[i]
		// if there is a json/jsonb column with "is-ext": true
		// assure it's the only one, or let error occurs
		var colSpec *ColumnSpec
		colSpec, err = parseColumnComment(col.Comment)
		if err != nil {
			return nil, mxerror.CommonErrorf("the comment: %s of column: %s is not a legal JSON-formatted string: %v", col.Name, col.Comment, err)
		}
		table.ColumnSpecs[i] = colSpec
		if colSpec != nil && colSpec.IsExt {
			if extCol != nil {
				return nil, mxerror.CommonErrorf("more than one column are designated as \"ext\" column")
			}
			if !isSupportedExtType(col.TypeName) {
				return nil, mxerror.CommonErrorf("an unsupported column is designated as \"ext\" column: %s of %s", col.Name, col.TypeName)
			}
			extCol = col
			extColSpec = colSpec
			continue
		}
	}

	// tolerate previous usage
	if extCol == nil && isSupportedExtType(table.Columns[len(table.Columns)-1].TypeName) {
		extCol = table.Columns[len(table.Columns)-1]
		extColSpec = table.ColumnSpecs[len(table.ColumnSpecs)-1]
	}

	if extCol != nil {
		table.ExtColumn = extCol
		// exclude the ext column, first
		table.TotalMetricsCount -= EXT_COLUMN_NUM

		table.ColumnNameExt = extCol.Name
		table.ColumnTypeExt = extCol.TypeName

		// check its "ColumnsDescription"
		if extColSpec != nil && len(extColSpec.ColumnsDescriptions) > 0 {
			table.ColumnsDescsExt = extColSpec.ColumnsDescriptions

			// reset metrics counts
			jsonMetricsCount := table.ColumnsDescsExt.getTotalMetricsCount()
			if jsonMetricsCount <= 1 {
				return nil, mxerror.CommonErrorf("the ext column %s accommodates one or fewer metrics: %d", extCol.Name, jsonMetricsCount)
			}
			table.JSONMetricsCount = jsonMetricsCount
			table.TotalMetricsCount += table.JSONMetricsCount
		}
	}
	return table, nil
}
