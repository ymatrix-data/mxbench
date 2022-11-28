package telematics

import (
	"fmt"

	"github.com/ymatrix-data/mxbench/internal/engine"
)

const _CUSTOM_QUERY_NAME_PREFIX = "CUSTOM_QUERY_"

type queryCustom struct {
	statement string
	number    int // from 1
}

func (q *queryCustom) GetSQL() string {
	return q.statement
}

func (q *queryCustom) GetName() string {
	return fmt.Sprintf("%s%d", _CUSTOM_QUERY_NAME_PREFIX, q.number)
}

func newQueryCustom(statement string, number int) engine.Query {
	return &queryCustom{statement: statement, number: number}
}
