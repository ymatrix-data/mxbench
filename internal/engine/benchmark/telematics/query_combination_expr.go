package telematics

import (
	"fmt"
	"time"

	"github.com/lib/pq"
	"github.com/ymatrix-data/mxbench/internal/engine/metadata"
)

type basicExpression struct {
	UseRawExpression bool   `json:"use-raw-expression"`
	Expression       string `json:"expression"`
}

type projections struct {
	basicExpression
}

func (p *projections) GetStr() string {
	if p.UseRawExpression {
		return p.Expression
	}
	return ""
}

type fromExpression struct {
	basicExpression

	UseRelationIdentifier bool   `json:"use-relation-identifier"`
	RelationIdentifier    string `json:"relation-identifier"`

	RelationStatement *queryCombination `json:"relation-statement"`

	HasAlias bool   `json:"has-alias"`
	Alias    string `json:"alias"`
}

func (f *fromExpression) GetStr() string {
	if f.UseRawExpression {
		return f.Expression
	}

	aliasExpr := ""
	if f.HasAlias {
		aliasExpr = "AS " + f.Alias
	}

	if f.UseRawExpression {
		return f.RelationIdentifier + aliasExpr
	}
	if f.UseRelationIdentifier {
		return f.RelationIdentifier + aliasExpr
	}

	if f.RelationStatement != nil {
		return "( " + f.RelationStatement.GetSQL() + " )" + aliasExpr
	}
	return ""
}

// predicate for preset columns, i.e. timestamp column and device id column
type basicPresetPredicate struct {
	basicExpression
	IsRandom bool   `json:"is-random"`
	HasAlias bool   `json:"has-alias"`
	Alias    string `json:"alias"`
}

type devicePredicate struct {
	basicPresetPredicate
	Count int `json:"count"`
}

func (d *devicePredicate) GetStr(meta *metadata.Metadata, cfg *Config) string {
	if d.UseRawExpression {
		return d.Expression
	}

	if !d.IsRandom {
		return ""
	}

	deviceColumnName := meta.Table.ColumnNameVIN
	if d.HasAlias {
		deviceColumnName = d.Alias
	}

	str := deviceColumnName
	if d.Count == 1 {
		str += "=" + meta.GetSingleVinGenerator()()
		return str
	}

	str += "\nIN ( " + meta.GetRandomVinsGenerator(d.Count)() + " )"
	return str
}

type timestampPredicate struct {
	basicPresetPredicate
	Duration int `json:"duration"`

	Start string `json:"start"`
	End   string `json:"end"`

	StartTime time.Time
	EndTime   time.Time

	StartExclusive bool `json:"start-exclusive"`
	EndExclusive   bool `json:"end-exclusive"`
}

func (t *timestampPredicate) GetStr(meta *metadata.Metadata, cfg *Config) string {
	if t.UseRawExpression {
		return t.Expression
	}

	tsColumnName := meta.Table.ColumnNameTS
	if t.HasAlias {
		tsColumnName = t.Alias
	}

	start, end, startOp, endOp := pq.QuoteLiteral(t.Start), pq.QuoteLiteral(t.End), ">=", "<="
	if t.StartExclusive {
		startOp = ">"
	}
	if t.EndExclusive {
		endOp = "<"
	}
	if t.IsRandom {
		start, end = meta.GetRandomStartEndTSArgGenerator(time.Duration(t.Duration * int(time.Second)))()
		if t.Duration == 0 {
			return tsColumnName + "=" + start
		}
	}
	if start == end {
		return tsColumnName + "=" + start
	}
	return fmt.Sprintf("%[1]s%[2]s%[3]s AND %[1]s%[4]s%[5]s", tsColumnName, startOp, start, endOp, end)
}

type metricsPredicate struct {
	basicExpression
}

func (m *metricsPredicate) GetStr() string {
	if m.UseRawExpression {
		return m.Expression
	}
	return ""
}

type groupByPredicate struct {
	basicExpression
}

func (g *groupByPredicate) GetStr() string {
	if g.UseRawExpression {
		return g.Expression
	}
	return ""
}

type orderByPredicate struct {
	basicExpression
}

func (o *orderByPredicate) GetStr() string {
	if o.UseRawExpression {
		return o.Expression
	}
	return ""
}
