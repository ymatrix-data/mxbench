package mxmock

import (
	"fmt"
	"math"
	"strconv"
	"strings"

	"github.com/brianvoe/gofakeit/v6"
)

type Float4 struct {
	BaseType
}

func NewFloat4(colName string) Type {
	return &Float4{
		BaseType: NewBaseType(colName),
	}
}

func (f4 *Float4) Random(keys ...string) string {
	for _, key := range keys {
		if key != f4.colName {
			continue
		}
		return fmt.Sprintf("%f", gofakeit.Float32Range(-2767, 2767))
	}
	return ""
}

func (f4 *Float4) ValueRange() map[string]*ValueRange {
	return nil
}

type Float4s struct {
	BaseType
	float4s []*Float4
}

func NewFloat4s(colName string) Type {
	return &Float4s{
		BaseType: NewBaseType(colName),
		float4s:  []*Float4{NewFloat4(colName).(*Float4)},
	}
}

func (f4s Float4s) Random(keys ...string) string {
	for _, key := range keys {
		if key != f4s.colName {
			continue
		}
		a := []string{}
		for _, f4 := range f4s.float4s {
			a = append(a, f4.Random(keys...))
		}
		return "{" + strings.Join(a, ",") + "}"
	}
	return "{}"
}

func (f4s Float4s) Parse(string) {}

func (f4s *Float4s) ValueRange() map[string]*ValueRange {
	return nil
}

type Float8 struct {
	BaseType
}

func NewFloat8(colName string) Type {
	return &Float8{
		BaseType: NewBaseType(colName),
	}
}

func (f8 *Float8) Random(keys ...string) string {
	for _, key := range keys {
		if key != f8.colName {
			continue
		}
		return fmt.Sprintf("%f", gofakeit.Float32Range(-2767, 2767))
	}
	return ""
}

func (f8 *Float8) ValueRange() map[string]*ValueRange {
	return nil
}

type Float8s struct {
	BaseType
	float8s []*Float8
}

func NewFloat8s(colName string) Type {
	return &Float8s{
		BaseType: NewBaseType(colName),
		float8s:  []*Float8{NewFloat8(colName).(*Float8)},
	}
}

func (f8s Float8s) Random(keys ...string) string {
	for _, key := range keys {
		if key != f8s.colName {
			continue
		}
		a := []string{}
		for _, f8 := range f8s.float8s {
			a = append(a, f8.Random(keys...))
		}
		return "{" + strings.Join(a, ",") + "}"
	}
	return "{}"
}

func (f8s Float8s) Parse(string) {}

func (f8s *Float8s) ValueRange() map[string]*ValueRange {
	return nil
}

type Numeric struct {
	BaseType
	Max       int
	Precision int
}

func NewNumeric(colName string) Type {
	return &Numeric{
		BaseType: NewBaseType(colName),
	}
}

func (n *Numeric) Parse(td string) {
	n.Max, n.Precision, _ = n.FloatPrecision(td)
}

func (n *Numeric) Random(keys ...string) string {
	for _, key := range keys {
		if key != n.colName {
			continue
		}
		fmtStr := "%." + fmt.Sprintf("%d", n.Precision) + "f"
		f := gofakeit.Float64Range(1, float64(n.Max))
		stringFloat := strconv.FormatFloat(f, 'f', n.Precision, 64)
		if len(stringFloat) > n.Max {
			f = math.Log10(f)
		}
		return fmt.Sprintf(fmtStr, f)
	}
	return ""
}

func (n *Numeric) ValueRange() map[string]*ValueRange {
	return nil
}

type Numerics struct {
	BaseType
	numerics []*Numeric
}

func NewNumerics(colName string) Type {
	return &Numerics{
		BaseType: NewBaseType(colName),
		numerics: []*Numeric{NewNumeric(colName).(*Numeric)},
	}
}

func (ns Numerics) Random(keys ...string) string {
	for _, key := range keys {
		if key != ns.colName {
			continue
		}
		a := []string{}
		for _, n := range ns.numerics {
			a = append(a, n.Random(keys...))
		}
		return "{" + strings.Join(a, ",") + "}"
	}
	return "{}"
}

func (ns Numerics) Parse(td string) {
	for _, n := range ns.numerics {
		n.Parse(td)
	}
}

func (ns *Numerics) ValueRange() map[string]*ValueRange {
	return nil
}

type Money struct {
	BaseType
}

func NewMoney(colName string) Type {
	return &Money{
		BaseType: NewBaseType(colName),
	}
}

func (m *Money) Random(keys ...string) string {
	for _, key := range keys {
		if key != m.colName {
			continue
		}
		return fmt.Sprintf("%f", gofakeit.Float32Range(-2767, 2767))
	}
	return ""
}

func (m *Money) ValueRange() map[string]*ValueRange {
	return nil
}

type Moneys struct {
	BaseType
	moneys []*Money
}

func NewMoneys(colName string) Type {
	return &Moneys{
		BaseType: NewBaseType(colName),
		moneys:   []*Money{NewMoney(colName).(*Money)},
	}
}

func (ms Moneys) Random(keys ...string) string {
	for _, key := range keys {
		if key != ms.colName {
			continue
		}
		a := []string{}
		for _, m := range ms.moneys {
			a = append(a, m.Random(keys...))
		}
		return "{" + strings.Join(a, ",") + "}"
	}
	return "{}"
}

func (ms Moneys) Parse(string) {}

func (ms *Moneys) ValueRange() map[string]*ValueRange {
	return nil
}
