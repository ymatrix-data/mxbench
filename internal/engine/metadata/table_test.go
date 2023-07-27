package metadata

import (
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("Mars2 Table", func() {
	It("options to string should be ok", func() {
		t := Options{
			{
				Name:  "compress_threshold",
				Value: 1000,
			},
			{
				Name:  "chunk_size",
				Value: 32,
			},
			{
				Name:  "xxxx",
				Value: "xxx",
			},
		}
		str := t.ToSQLStr()
		Expect(str).To(Equal("compress_threshold='1000', chunk_size='32', xxxx='xxx'"))
	})
	It("should have two column with empty configuration", func() {
		t, _ := NewMarsTable(&Config{}, StorageMars2)
		Expect(t.Options).To(HaveLen(2))
		Expect(t.Options[0].Value).To(Equal(1000))
		Expect(t.Options[1].Value).To(Equal(32))
		Expect(t.Columns).To(HaveLen(2))
		Expect(t.Columns[0].Name).To(Equal("ts"))
		Expect(t.Columns[0].TypeName).To(Equal(ColumnTypeTimestamp))
		Expect(t.Columns[1].Name).To(Equal("vin"))
		Expect(t.Columns[1].TypeName).To(Equal(ColumnTypeText))
		Expect(t.Storage).To(Equal(StorageMars2))
		Expect(t.DistKey).To(Equal(ColumnNameVIN))
		Expect(t.OrderByKey).To(Equal([]string{""}))
		Expect(t.Indexes).To(HaveLen(1))
	})
	It("should create table without json column", func() {
		t, _ := NewMarsTable(&Config{
			TableName:         "xx",
			TagNum:            100,
			MetricsType:       MetricsTypeFloat4,
			TotalMetricsCount: 998,
		}, StorageMars2)

		Expect(t.Options).To(HaveLen(2))
		Expect(t.Options[0].Value).To(Equal(1000))
		Expect(t.Options[1].Value).To(Equal(32))

		Expect(t.Columns).To(HaveLen(1000))
		Expect(t.Columns[0].Name).To(Equal("ts"))
		Expect(t.Columns[0].TypeName).To(Equal(ColumnTypeTimestamp))
		Expect(t.Columns[1].Name).To(Equal("vin"))
		Expect(t.Columns[1].TypeName).To(Equal(ColumnTypeText))
		Expect(t.Columns[2].Name).To(Equal("c0"))
		Expect(t.Columns[2].TypeName).To(Equal(MetricsTypeFloat4))

		Expect(t.Columns[999].Name).To(Equal("c997"))
		Expect(t.Columns[999].TypeName).To(Equal(MetricsTypeFloat4))

		Expect(t.Storage).To(Equal(StorageMars2))
		Expect(t.DistKey).To(Equal(ColumnNameVIN))
		Expect(t.OrderByKey).To(Equal([]string{""}))
		Expect(t.Indexes).To(HaveLen(1))
	})
	It("should create table with json column", func() {
		t, _ := NewMarsTable(&Config{
			TableName:         "xx",
			TagNum:            100,
			MetricsType:       MetricsTypeFloat4,
			TotalMetricsCount: 999,
		}, StorageMars2)

		Expect(t.Options).To(HaveLen(2))
		Expect(t.Options[0].Value).To(Equal(1000))
		Expect(t.Options[1].Value).To(Equal(32))

		Expect(t.Columns).To(HaveLen(1000))
		Expect(t.Columns[0].Name).To(Equal("ts"))
		Expect(t.Columns[0].TypeName).To(Equal(ColumnTypeTimestamp))
		Expect(t.Columns[1].Name).To(Equal("vin"))
		Expect(t.Columns[1].TypeName).To(Equal(ColumnTypeText))
		Expect(t.Columns[2].Name).To(Equal("c0"))
		Expect(t.Columns[2].TypeName).To(Equal(MetricsTypeFloat4))

		Expect(t.Columns[998].Name).To(Equal("c996"))
		Expect(t.Columns[998].TypeName).To(Equal(MetricsTypeFloat4))

		Expect(t.Columns[999].Name).To(Equal("ext"))
		Expect(t.Columns[999].TypeName).To(Equal(MetricsTypeJSON))

		Expect(t.Storage).To(Equal(StorageMars2))
		Expect(t.DistKey).To(Equal(ColumnNameVIN))
		Expect(t.OrderByKey).To(Equal([]string{""}))
		Expect(t.Indexes).To(HaveLen(1))
	})
	It("should ...", func() {
	})
})

var _ = Describe("Mars3 Table", func() {
	It("options to string should be ok", func() {
		t := Options{
			{
				Name:  "compresstype",
				Value: "lz4",
			},
			{
				Name:  "compresslevel",
				Value: 1,
			},
		}
		str := t.ToSQLStr()
		Expect(str).To(Equal("compress_threshold='lz4', compresslevel=1"))
	})
	It("should have two column with empty configuration", func() {
		t, _ := NewMarsTable(&Config{}, StorageMars3)
		Expect(t.Options).To(HaveLen(2))
		Expect(t.Options[0].Value).To(Equal(1000))
		Expect(t.Options[1].Value).To(Equal(32))
		Expect(t.Columns).To(HaveLen(2))
		Expect(t.Columns[0].Name).To(Equal("ts"))
		Expect(t.Columns[0].TypeName).To(Equal(ColumnTypeTimestamp))
		Expect(t.Columns[1].Name).To(Equal("vin"))
		Expect(t.Columns[1].TypeName).To(Equal(ColumnTypeText))
		Expect(t.Storage).To(Equal(StorageMars3))
		Expect(t.DistKey).To(Equal(ColumnNameVIN))
		Expect(t.OrderByKey).To(Equal([]string{ColumnNameVIN, ColumnNameTS}))
		Expect(t.Indexes).To(HaveLen(1))
	})
	It("should create table without json column", func() {
		t, _ := NewMarsTable(&Config{
			TableName:         "xx",
			TagNum:            100,
			MetricsType:       MetricsTypeFloat4,
			TotalMetricsCount: 998,
		}, StorageMars3)

		Expect(t.Options).To(HaveLen(2))
		Expect(t.Options[0].Value).To(Equal(1000))
		Expect(t.Options[1].Value).To(Equal(32))

		Expect(t.Columns).To(HaveLen(1000))
		Expect(t.Columns[0].Name).To(Equal("ts"))
		Expect(t.Columns[0].TypeName).To(Equal(ColumnTypeTimestamp))
		Expect(t.Columns[1].Name).To(Equal("vin"))
		Expect(t.Columns[1].TypeName).To(Equal(ColumnTypeText))
		Expect(t.Columns[2].Name).To(Equal("c0"))
		Expect(t.Columns[2].TypeName).To(Equal(MetricsTypeFloat4))

		Expect(t.Columns[999].Name).To(Equal("c997"))
		Expect(t.Columns[999].TypeName).To(Equal(MetricsTypeFloat4))

		Expect(t.Storage).To(Equal(StorageMars3))
		Expect(t.DistKey).To(Equal(ColumnNameVIN))
		Expect(t.OrderByKey).To(Equal([]string{ColumnNameVIN, ColumnNameTS}))
		Expect(t.Indexes).To(HaveLen(1))
	})
	It("should create table with json column", func() {
		t, _ := NewMarsTable(&Config{
			TableName:         "xx",
			TagNum:            100,
			MetricsType:       MetricsTypeFloat4,
			TotalMetricsCount: 999,
		}, StorageMars3)

		Expect(t.Options).To(HaveLen(2))
		Expect(t.Options[0].Value).To(Equal(1000))
		Expect(t.Options[1].Value).To(Equal(32))

		Expect(t.Columns).To(HaveLen(1000))
		Expect(t.Columns[0].Name).To(Equal("ts"))
		Expect(t.Columns[0].TypeName).To(Equal(ColumnTypeTimestamp))
		Expect(t.Columns[1].Name).To(Equal("vin"))
		Expect(t.Columns[1].TypeName).To(Equal(ColumnTypeText))
		Expect(t.Columns[2].Name).To(Equal("c0"))
		Expect(t.Columns[2].TypeName).To(Equal(MetricsTypeFloat4))

		Expect(t.Columns[998].Name).To(Equal("c996"))
		Expect(t.Columns[998].TypeName).To(Equal(MetricsTypeFloat4))

		Expect(t.Columns[999].Name).To(Equal("ext"))
		Expect(t.Columns[999].TypeName).To(Equal(MetricsTypeJSON))

		Expect(t.Storage).To(Equal(StorageMars3))
		Expect(t.DistKey).To(Equal(ColumnNameVIN))
		Expect(t.OrderByKey).To(Equal([]string{ColumnNameVIN, ColumnNameTS}))
		Expect(t.Indexes).To(HaveLen(1))
	})
	It("should ...", func() {
	})
})
