package metadata

import (
	"time"

	"github.com/ymatrix-data/mxbench/internal/util"
	"github.com/ymatrix-data/mxbench/internal/util/mxerror"
)

type Config struct {
	SchemaName              string
	TableName               string
	TagNum                  int64
	StartAt                 time.Time
	EndAt                   time.Time
	PartitionIntervalInHour int64
	MetricsType             MetricsType
	StorageType             StorageType
	TotalMetricsCount       int64
	MetricsDescriptions     string
	TimestampStepInSecond   uint64
	HasUniqueConstraints    bool
	EmptyValueRatio         int
	IsDDLFromFile           bool
	DB                      util.DBConnParams
	DBVersion               util.DBVersion
}

func (cfg *Config) validate() error {
	if cfg.SchemaName == "" {
		return mxerror.CommonError("SchemaName should not be nil")
	}
	// Since the parameter "table-name" is not assigned any default value,
	// we should provide more friendly hint infomation for the users.
	if cfg.TableName == "" {
		return mxerror.CommonError("Please designate your target table for test with the parameter 'table-name'")
	}
	if cfg.TagNum <= 0 {
		return mxerror.CommonError("Tag Num must be bigger than 0")
	}
	if cfg.TimestampStepInSecond == 0 {
		return mxerror.CommonError("TimestampStepInSecond should not be 0")
	}
	if cfg.MetricsType == "" {
		return mxerror.CommonError("must have MetricsType")
	}
	if !cfg.StartAt.Before(cfg.EndAt) {
		return mxerror.CommonErrorf("startAt(%s) should before endAt(%s)", cfg.StartAt, cfg.EndAt)
	}
	return nil
}
