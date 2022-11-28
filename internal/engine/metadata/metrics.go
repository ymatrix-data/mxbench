package metadata

type MetricsType = ColumnType

const (
	MetricsTypeFloat4 MetricsType = "float4"
	MetricsTypeFloat8 MetricsType = "float8"
	MetricsTypeInt4   MetricsType = "int4"
	MetricsTypeInt8   MetricsType = "int8"
	MetricsTypeJSON   MetricsType = "json"
	MetricsTypeJSONB  MetricsType = "jsonb"
)

func Size(t MetricsType) int64 {
	switch t {
	case MetricsTypeInt4:
		return 4
	case MetricsTypeInt8:
		return 8
	case MetricsTypeFloat4:
		return 4
	case MetricsTypeFloat8:
		return 8
	default:
		return 8
	}
}
