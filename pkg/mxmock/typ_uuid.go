package mxmock

import (
	"strings"

	"github.com/brianvoe/gofakeit/v6"
)

type UUID struct {
	BaseType
}

func NewUUID(colName string) Type {
	return &UUID{
		BaseType: NewBaseType(colName),
	}
}

func (u *UUID) Random(keys ...string) string {
	for _, key := range keys {
		if key != u.colName {
			continue
		}
		return gofakeit.UUID()
	}
	return ""
}

func (u *UUID) ValueRange() map[string]*ValueRange {
	return nil
}

type UUIDs struct {
	BaseType
	uuids []*UUID
}

func NewUUIDs(colName string) Type {
	return &UUIDs{
		BaseType: NewBaseType(colName),
		uuids:    []*UUID{NewUUID(colName).(*UUID)},
	}
}

func (us UUIDs) Random(keys ...string) string {
	for _, key := range keys {
		if key != us.colName {
			continue
		}
		a := []string{}
		for _, u := range us.uuids {
			a = append(a, u.Random(keys...))
		}

		return "{" + strings.Join(a, ",") + "}"
	}
	return "{}"
}

func (us UUIDs) Parse(string) {}

func (us *UUIDs) ValueRange() map[string]*ValueRange {
	return nil
}
