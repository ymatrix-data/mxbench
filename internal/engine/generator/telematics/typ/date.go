package typ

import (
	"sync"
	"time"
	"math/rand"

	"github.com/ymatrix-data/mxbench/internal/engine/metadata"
	"github.com/ymatrix-data/mxbench/pkg/mxmock"
)

type Date struct {
	mxmock.BaseType
	columnSpec *metadata.ColumnSpec

	valueRanges *mxmock.ValueRange
	mu          sync.RWMutex
}

func GetNewDate(table *metadata.Table) func(string) mxmock.Type {
	return func(colName string) mxmock.Type {
		dt := &Date{
			BaseType: mxmock.NewBaseType(colName),
		}
		for colInd, col := range table.Columns {
			if col.Name == colName {
				dt.columnSpec = table.ColumnSpecs[colInd]
				break
			}
		}
		return dt
	}
}

func (dt *Date) Random(keys ...string) string {
	if dt.columnSpec != nil && dt.columnSpec.RangedDate {
		start, err1 := time.Parse("2006-01-02", dt.columnSpec.DateStart)
        if err1 != nil {
            panic(err1);
        }

        end, err2 := time.Parse("2006-01-02", dt.columnSpec.DateEnd)
        if err2 != nil {
            panic(err2)
        }

		if end.Before(start) {
            panic("end date is before start date")
        }

		delta := end.Sub(start)
        sec := rand.Int63n(int64(delta / time.Second))

        randomTime := start.Add(time.Duration(sec) * time.Second)
        return randomTime.Format("2006-01-02")
	} else if dt.columnSpec != nil && dt.columnSpec.Fixed {
		return dt.columnSpec.FixedValue
	}

	return "YYYYYYYYY"
}

func (dt *Date) ValueRange() map[string]*mxmock.ValueRange {
	dt.mu.RLock()
	defer dt.mu.RUnlock()

	return map[string]*mxmock.ValueRange{
		"date": dt.valueRanges,
	}
}
