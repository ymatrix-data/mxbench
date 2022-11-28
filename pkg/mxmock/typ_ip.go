package mxmock

import (
	"strings"

	"github.com/brianvoe/gofakeit/v6"
)

type INet struct {
	BaseType
}

func NewINet(colName string) Type {
	return &INet{
		BaseType: NewBaseType(colName),
	}
}

func (i *INet) Random(keys ...string) string {
	for _, key := range keys {
		if key != i.colName {
			continue
		}
		return gofakeit.IPv6Address()
	}
	return ""
}

type INets struct {
	BaseType
	inets []*INet
}

func NewINets(colName string) Type {
	return &INets{
		BaseType: NewBaseType(colName),
		inets:    []*INet{NewINet(colName).(*INet)},
	}
}

func (is INets) Random(keys ...string) string {
	for _, key := range keys {
		if key != is.colName {
			continue
		}
		a := []string{}

		for _, i := range is.inets {

			a = append(a, i.Random(keys...))
		}
		return "{" + strings.Join(a, ",") + "}"
	}
	return "{}"
}

func (is INets) Parse(string) {}

type CIDR struct {
	BaseType
}

func NewCIDR(colName string) Type {
	return &CIDR{
		BaseType: NewBaseType(colName),
	}
}

func (c *CIDR) Random(keys ...string) string {
	for _, key := range keys {
		if key != c.colName {
			continue
		}
		return gofakeit.IPv6Address()
	}
	return ""
}

type CIDRs struct {
	BaseType
	cidrs []*CIDR
}

func NewCIDRs(colName string) Type {
	return &CIDRs{
		BaseType: NewBaseType(colName),
		cidrs:    []*CIDR{NewCIDR(colName).(*CIDR)},
	}
}

func (cs CIDRs) Random(keys ...string) string {
	for _, key := range keys {
		if key != cs.colName {
			continue
		}
		a := []string{}

		for _, c := range cs.cidrs {

			a = append(a, c.Random(keys...))
		}
		return "{" + strings.Join(a, ",") + "}"
	}
	return "{}"
}

func (cs CIDRs) Parse(string) {}
