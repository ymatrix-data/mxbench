package mxmock

import (
	"strings"

	"github.com/brianvoe/gofakeit/v6"
)

type Date struct {
	BaseType
}

func NewDate(colName string) Type {
	return &Date{
		BaseType: NewBaseType(colName),
	}
}

func (d *Date) Random(keys ...string) string {
	for _, key := range keys {
		if key != d.colName {
			continue
		}
		return gofakeit.Date().Format("2006-01-02")
	}
	return ""
}

type Dates struct {
	BaseType
	dates []*Date
}

func NewDates(colName string) Type {
	return &Dates{
		BaseType: NewBaseType(colName),
		dates:    []*Date{NewDate(colName).(*Date)},
	}
}

func (ds Dates) Random(keys ...string) string {
	for _, key := range keys {
		if key != ds.colName {
			continue
		}
		a := []string{}
		for _, d := range ds.dates {
			a = append(a, d.Random(keys...))
		}
		return "{" + strings.Join(a, ",") + "}"
	}
	return "{}"
}

func (ds Dates) Parse(string) {}

type Time struct {
	BaseType
}

func NewTime(colName string) Type {
	return &Time{
		BaseType: NewBaseType(colName),
	}
}

func (t Time) Random(keys ...string) string {
	for _, key := range keys {
		if key != t.colName {
			continue
		}
		return gofakeit.Date().Format("15:04:05")
	}
	return ""
}

type Times struct {
	BaseType
	times []*Time
}

func NewTimes(colName string) Type {
	return &Times{
		BaseType: NewBaseType(colName),
		times:    []*Time{NewTime(colName).(*Time)},
	}
}

func (ts Times) Random(keys ...string) string {
	for _, key := range keys {
		if key != ts.colName {
			continue
		}
		a := []string{}
		for _, t := range ts.times {
			a = append(a, t.Random(keys...))
		}
		return "{" + strings.Join(a, ",") + "}"
	}
	return "{}"
}

func (ts Times) Parse(string) {}

type TimeTZ struct {
	BaseType
}

func NewTimeTZ(colName string) Type {
	return &TimeTZ{
		BaseType: NewBaseType(colName),
	}
}

func (t *TimeTZ) Random(keys ...string) string {
	for _, key := range keys {
		if key != t.colName {
			continue
		}
		return gofakeit.Date().Format("15:04:05.000000")
	}
	return ""
}

type TimeTZs struct {
	BaseType
	timeTZs []*TimeTZ
}

func NewTimeTZs(colName string) Type {
	return &TimeTZs{
		BaseType: NewBaseType(colName),
		timeTZs:  []*TimeTZ{NewTimeTZ(colName).(*TimeTZ)},
	}
}

func (ts TimeTZs) Random(keys ...string) string {
	for _, key := range keys {
		if key != ts.colName {
			continue
		}
		a := []string{}
		for _, t := range ts.timeTZs {
			a = append(a, t.Random(keys...))
		}
		return "{" + strings.Join(a, ",") + "}"
	}
	return "{}"
}

func (ts TimeTZs) Parse(string) {}

type Timestamp struct {
	BaseType
}

func NewTimestamp(colName string) Type {
	return &Timestamp{
		BaseType: NewBaseType(colName),
	}
}

func (t *Timestamp) Random(keys ...string) string {
	for _, key := range keys {
		if key != t.colName {
			continue
		}
		return gofakeit.Date().Format("2006-01-02 15:04:05")
	}
	return ""
}

type Timestamps struct {
	BaseType
	timestamps []*Timestamp
}

func NewTimestamps(colName string) Type {
	return &Timestamps{
		BaseType:   NewBaseType(colName),
		timestamps: []*Timestamp{NewTimestamp(colName).(*Timestamp)},
	}
}

func (ts Timestamps) Random(keys ...string) string {
	for _, key := range keys {
		if key != ts.colName {
			continue
		}
		a := []string{}
		for _, t := range ts.timestamps {
			a = append(a, t.Random(keys...))
		}
		return "{" + strings.Join(a, ",") + "}"
	}
	return "{}"
}

func (ts Timestamps) Parse(string) {}

type TimestampTZ struct {
	BaseType
}

func NewTimestampTZ(colName string) Type {
	return &TimestampTZ{
		BaseType: NewBaseType(colName),
	}
}

func (t *TimestampTZ) Random(keys ...string) string {
	for _, key := range keys {
		if key != t.colName {
			continue
		}
		return gofakeit.Date().Format("2006-01-02 15:04:05.000000")
	}
	return ""
}

type TimestampTZs struct {
	BaseType
	timestampTZs []*TimestampTZ
}

func NewTimestampTZs(colName string) Type {
	return &TimestampTZs{
		BaseType:     NewBaseType(colName),
		timestampTZs: []*TimestampTZ{NewTimestampTZ(colName).(*TimestampTZ)},
	}
}

func (ts TimestampTZs) Random(keys ...string) string {
	for _, key := range keys {
		if key != ts.colName {
			continue
		}
		a := []string{}
		for _, t := range ts.timestampTZs {
			a = append(a, t.Random(keys...))
		}
		return "{" + strings.Join(a, ",") + "}"
	}
	return "{}"
}

func (ts TimestampTZs) Parse(string) {}
