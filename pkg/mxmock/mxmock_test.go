package mxmock

import (
	"fmt"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("Mxmock", func() {
	Context("mock YMatrix data", func() {
		It("should mock row data", func() {
			_, err := NewMXMockerFromColumns([]*Column{
				{
					Name:     "c9",
					TypeName: "catalog_status",
					TypeDesc: "mxgate_internal.catalog_status",
				},
			})
			Expect(err).To(HaveOccurred())

			mocker, err := NewMXMockerFromColumns([]*Column{
				{
					Name:     "c1",
					TypeName: "int4",
					TypeDesc: "integer",
					DefVal:   "nextval('mxgate_internal.catalog_id_seq'::regclass)",
				},
				{
					Name:     "c2",
					TypeName: "int2",
					TypeDesc: "smallint",
				},
				{
					Name:     "c3",
					TypeName: "_int2",
					TypeDesc: "smallint[]",
				},
				{
					Name:     "c4",
					TypeName: "int4",
					TypeDesc: "integer",
				},
				{
					Name:     "c5",
					TypeName: "_int4",
					TypeDesc: "integer[]",
				},
				{
					Name:     "c6",
					TypeName: "int8",
					TypeDesc: "bigint",
				},
				{
					Name:     "c7",
					TypeName: "_int8",
					TypeDesc: "bigint[]",
				},

				{
					Name:     "c8",
					TypeName: "timestamptz",
					TypeDesc: "timestamp with time zone",
					DefVal:   "CURRENT_TIMESTAMP",
				},
				{
					Name:     "c10",
					TypeName: "text",
					TypeDesc: "text",
				},
				{
					Name:     "c11",
					TypeName: "int8",
					TypeDesc: "bigint",
				},
				{
					Name:     "c12",
					TypeName: "oid",
					TypeDesc: "oid",
				},
				{
					Name:     "c13",
					TypeName: "float4",
					TypeDesc: "real",
				},
				{
					Name:     "c14",
					TypeName: "_float4",
					TypeDesc: "real[]",
				},
				{
					Name:     "c15",
					TypeName: "float8",
					TypeDesc: "double precision",
				},
				{
					Name:     "c16",
					TypeName: "_float8",
					TypeDesc: "double precision[]",
				},
				{
					Name:     "c17",
					TypeName: "numeric",
					TypeDesc: "numeric(4,3)",
				},
				{
					Name:     "c18",
					TypeName: "_numeric",
					TypeDesc: "numeric(4,3)[]",
				},
				{
					Name:     "c19",
					TypeName: "money",
					TypeDesc: "money",
				},
				{
					Name:     "c20",
					TypeName: "_money",
					TypeDesc: "money[]",
				},
				{
					Name:     "c21",
					TypeName: "bit",
					TypeDesc: "bit(1)",
				},
				{
					Name:     "c22",
					TypeName: "_bit",
					TypeDesc: "bit(1)[]",
				},
				{
					Name:     "c23",
					TypeName: "varbit",
					TypeDesc: "varbit(4)",
				},
				{
					Name:     "c23",
					TypeName: "_varbit",
					TypeDesc: "varbit(10)[]",
				},
				{
					Name:     "c24",
					TypeName: "bool",
					TypeDesc: "boolean",
				},
				{
					Name:     "c25",
					TypeName: "_bool",
					TypeDesc: "boolean[]",
				},
				{
					Name:     "c26",
					TypeName: "bpchar",
					TypeDesc: "character(10)",
				},
				{
					Name:     "c27",
					TypeName: "_bpchar",
					TypeDesc: "character(10)[]",
				},
				{
					Name:     "c28",
					TypeName: "varchar",
					TypeDesc: "character varying(10)",
				},
				{
					Name:     "c29",
					TypeName: "_varchar",
					TypeDesc: "character varying(10)[]",
				},
				{
					Name:     "c30",
					TypeName: "text",
					TypeDesc: "text",
				},
				{
					Name:     "c31",
					TypeName: "_text",
					TypeDesc: "text[]",
				},
				{
					Name:     "c32",
					TypeName: "inet",
					TypeDesc: "inet",
				},
				{
					Name:     "c33",
					TypeName: "_inet",
					TypeDesc: "inet[]",
				},
				{
					Name:     "c34",
					TypeName: "cidr",
					TypeDesc: "cidr",
				},
				{
					Name:     "c35",
					TypeName: "_cidr",
					TypeDesc: "cidr[]",
				},
				{
					Name:     "c36",
					TypeName: "time",
					TypeDesc: "time without time zone",
				},
				{
					Name:     "c37",
					TypeName: "_time",
					TypeDesc: "time without time zone[]",
				},
				{
					Name:     "c38",
					TypeName: "interval",
					TypeDesc: "interval",
				},
				{
					Name:     "c39",
					TypeName: "_interval",
					TypeDesc: "interval[]",
				},
				{
					Name:     "c40",
					TypeName: "date",
					TypeDesc: "date",
				},
				{
					Name:     "c41`",
					TypeName: "_date",
					TypeDesc: "date[]",
				},
				{
					Name:     "c42",
					TypeName: "timetz",
					TypeDesc: "time with time zone",
				},
				{
					Name:     "c43",
					TypeName: "_timetz",
					TypeDesc: "time with time zone[]",
				},
				{
					Name:     "c44",
					TypeName: "timestamp",
					TypeDesc: "timestamp without time zone",
				},
				{
					Name:     "c45",
					TypeName: "_timestamp",
					TypeDesc: "timestamp without time zone[]",
				},
				{
					Name:     "c46",
					TypeName: "timestamptz",
					TypeDesc: "timestamp with time zone",
				},
				{
					Name:     "c47",
					TypeName: "_timestamptz",
					TypeDesc: "timestamp with time zone[]",
				},
				{
					Name:     "c48",
					TypeName: "box",
					TypeDesc: "box",
				},
				{
					Name:     "c49",
					TypeName: "_box",
					TypeDesc: "box[]",
				},
				{
					Name:     "c50",
					TypeName: "circle",
					TypeDesc: "circle",
				},
				{
					Name:     "c51",
					TypeName: "_circle",
					TypeDesc: "circle[]",
				},
				{
					Name:     "c52",
					TypeName: "line",
					TypeDesc: "line",
				},
				{
					Name:     "c53",
					TypeName: "_line",
					TypeDesc: "line[]",
				},
				{
					Name:     "c54",
					TypeName: "lseg",
					TypeDesc: "lseg",
				},
				{
					Name:     "c55",
					TypeName: "_lseg",
					TypeDesc: "lseg[]",
				},
				{
					Name:     "c56",
					TypeName: "path",
					TypeDesc: "path",
				},
				{
					Name:     "c57",
					TypeName: "_path",
					TypeDesc: "path[]",
				},
				{
					Name:     "c58",
					TypeName: "polygon",
					TypeDesc: "polygon",
				},
				{
					Name:     "c59",
					TypeName: "_polygon",
					TypeDesc: "polygon[]",
				},
				{
					Name:     "c60",
					TypeName: "point",
				},
				{
					Name:     "c61",
					TypeName: "point",
				},
				{
					Name:     "c62",
					TypeName: "json",
				},
				{
					Name:     "c63",
					TypeName: "_json",
				},
				{
					Name:     "c64",
					TypeName: "jsonb",
				},
				{
					Name:     "c65",
					TypeName: "_jsonb",
				},
				{
					Name:     "c66",
					TypeName: "xml",
				},
				{
					Name:     "c67",
					TypeName: "_xml",
				},
				{
					Name:     "c68",
					TypeName: "macaddr",
				},
				{
					Name:     "c69",
					TypeName: "_macaddr",
				},
				{
					Name:     "c70",
					TypeName: "tsquery",
				},
				{
					Name:     "c71",
					TypeName: "_tsquery",
				},
				{
					Name:     "c72",
					TypeName: "tsvector",
				},
				{
					Name:     "c73",
					TypeName: "_tsvector",
				},
				{
					Name:     "c74",
					TypeName: "uuid",
				},
				{
					Name:     "c75",
					TypeName: "_uuid",
				},
				{
					Name:     "c76",
					TypeName: "bytea",
				},
				{
					Name:     "c77",
					TypeName: "_bytea",
				},
				{
					Name:     "c78",
					TypeName: "pg_lsn",
				},
				{
					Name:     "c79",
					TypeName: "_pg_lsn",
				},
				{
					Name:     "c80",
					TypeName: "txid_snapshot",
				},
				{
					Name:     "c81",
					TypeName: "_txid_snapshot",
				},
			})

			Expect(err).NotTo(HaveOccurred())

			err = mocker.init()
			Expect(err).NotTo(HaveOccurred())

			row := mocker.MockRow()
			Expect(row).To(HaveLen(80))
			for idx, column := range row {
				fmt.Println(idx, ":", column)
			}

			_, err = mocker.MockBatch(20, 50)
			Expect(err).To(HaveOccurred())

			_, err = mocker.MockBatch(0, 100)
			Expect(err).To(HaveOccurred())

			rows, err := mocker.MockBatch(12, 5)
			Expect(err).NotTo(HaveOccurred())
			Expect(rows).To(HaveLen(12))

			for _, row := range rows {
				Expect(row).To(HaveLen(80))
			}

			mocker.ExcludeColumn("c1", "c2")

			rows, err = mocker.MockBatch(12, 5)
			Expect(err).NotTo(HaveOccurred())
			Expect(rows).To(HaveLen(12))

			for _, row := range rows {
				Expect(row).To(HaveLen(79))
			}

			mocker, err = NewMXMockerFromColumns([]*Column{
				{
					Name:     "c1",
					TypeName: "_text",
					TypeDesc: "text[]",
				},
			})
			Expect(err).NotTo(HaveOccurred())

			err = mocker.init()
			Expect(err).NotTo(HaveOccurred())

			row = mocker.MockRow()
			Expect(row).To(HaveLen(1))
			for idx, column := range row {
				fmt.Println(idx, ":", column)
			}
		})
	})
})
