package mxmock

import (
	"fmt"
	"regexp"
	"strconv"
	"strings"
)

type Type interface {
	// Fake a random value for keys belong current type
	Random(keys ...string) string
	// Parse typedesc to current type's field.
	Parse(string)
	// Keys of this type have
	Keys() []string
}

type BaseType struct {
	colName string
}

func NewBaseType(colName string) BaseType {
	if colName == "" {
		panic("column name should not be null")
	}
	return BaseType{
		colName: colName,
	}
}

func (t *BaseType) Parse(string) {}

func (t *BaseType) Keys() []string {
	return []string{t.colName}
}

func (t *BaseType) GetColName() string {
	return t.colName
}

// Extract Float precision from the float typedesc
func (t *BaseType) FloatPrecision(td string) (int, int, error) {
	if !t.BracketsExists(td) {
		return 5, 3, nil
	}

	var rgx = regexp.MustCompile(`\((.*?)\)`)
	rs := rgx.FindStringSubmatch(td)
	split := strings.Split(rs[1], ",")
	m, err := strconv.Atoi(split[0])
	if err != nil {
		return 0, 0, fmt.Errorf("float Precision (min): %v", err)
	}
	p, err := strconv.Atoi(split[1])
	if err != nil {
		return 0, 0, fmt.Errorf("float Precision (precision): %v", err)
	}
	return m, p, nil
}

// If given a typedesc see if it has a bracket or not.
func (t *BaseType) BracketsExists(td string) bool {
	var rgx = regexp.MustCompile(`\(.*\)`)
	rs := rgx.FindStringSubmatch(td)
	return len(rs) > 0
}

// Extract total characters that the typedesc char can store.
func (t *BaseType) CharLen(td string) (int, error) {
	var rgx = regexp.MustCompile(`\((.*?)\)`)
	var returnValue int
	var err error
	rs := rgx.FindStringSubmatch(td)
	if len(rs) > 1 { // If the datatypes has number of value defined
		returnValue, err = strconv.Atoi(rs[1])
	} else {
		returnValue = 1
	}
	if err != nil {
		return 0, err
	}
	return returnValue, nil
}

type NewFunc func(string) Type

var TypMap = map[string]NewFunc{
	"int2":           NewInt2,
	"int4":           NewInt4,
	"_int2":          NewInt2s,
	"_int4":          NewInt4s,
	"int8":           NewInt8,
	"_int8":          NewInt8s,
	"oid":            NewOid,
	"float4":         NewFloat4,
	"_float4":        NewFloat4s,
	"float8":         NewFloat8,
	"_float8":        NewFloat8s,
	"numeric":        NewNumeric,
	"_numeric":       NewNumerics,
	"money":          NewMoney,
	"_money":         NewMoneys,
	"bit":            NewBit,
	"_bit":           NewBits,
	"varbit":         NewBit,
	"_varbit":        NewBits,
	"bpchar":         NewBPChar,
	"_bpchar":        NewBPChars,
	"varchar":        NewVarChar,
	"_varchar":       NewVarChars,
	"bool":           NewBool,
	"_bool":          NewBools,
	"text":           NewText,
	"_text":          NewTexts,
	"inet":           NewINet,
	"_inet":          NewINets,
	"cidr":           NewCIDR,
	"_cidr":          NewCIDRs,
	"time":           NewTime,
	"_time":          NewTimes,
	"interval":       NewTime,
	"_interval":      NewTimes,
	"date":           NewDate,
	"_date":          NewDates,
	"timetz":         NewTimeTZ,
	"_timetz":        NewTimeTZs,
	"timestamp":      NewTimestamp,
	"_timestamp":     NewTimestamps,
	"timestamptz":    NewTimestampTZ,
	"_timestamptz":   NewTimestampTZs,
	"box":            NewBox,
	"_box":           NewBoxes,
	"circle":         NewCircle,
	"_circle":        NewCircles,
	"line":           NewBox,
	"_line":          NewBoxes,
	"lseg":           NewBox,
	"_lseg":          NewBoxes,
	"path":           NewBox,
	"_path":          NewBoxes,
	"polygon":        NewBox,
	"_polygon":       NewBoxes,
	"point":          NewPoint,
	"_point":         NewPoints,
	"json":           NewJSON,
	"_json":          NewJSONs,
	"jsonb":          NewJSON,
	"_jsonb":         NewJSONs,
	"xml":            NewXML,
	"_xml":           NewXMLs,
	"macaddr":        NewMacAddr,
	"_macaddr":       NewMacAddrs,
	"tsquery":        NewTSQuery,
	"_tsquery":       NewTSQueries,
	"tsvector":       NewTsVector,
	"_tsvector":      NewTsVectors,
	"uuid":           NewUUID,
	"_uuid":          NewUUIDs,
	"bytea":          NewByteA,
	"_bytea":         NewByteAs,
	"pg_lsn":         NewPgLsn,
	"_pg_lsn":        NewPgLsns,
	"txid_snapshot":  NewTxidSnapshot,
	"_txid_snapshot": NewTxidSnapshots,
}
