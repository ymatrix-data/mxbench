package typ

import (
	"fmt"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"

	"github.com/ymatrix-data/mxbench/internal/engine/metadata"
)

var _ = Describe("JSON type", func() {
	table := &metadata.Table{
		Columns: metadata.Columns{
			{
				Name:     "c0",
				TypeName: "timestamp",
			},
			{
				Name:     "c1",
				TypeName: "text",
			},
			{
				Name:     "ext",
				TypeName: "json",
			},
		},
		ColumnNameExt: "ext",
		ColumnSpecs: metadata.ColumnSpecs{
			{},
			{},
			{},
		},
		JSONMetricsCount:         5,
		JSONMetricsCandidateType: "int8",
	}

	Context("JSON type", func() {
		It("get random value and then get value range", func() {
			jsonType := GetNewJSON(table)("ext")
			Expect(jsonType).ToNot(BeNil())

			keys := jsonType.Keys()
			value := jsonType.Random(keys...)
			valueRange := jsonType.ValueRange()

			fmt.Fprintf(GinkgoWriter, "keys: %+v, value: %v, value range: %v\n", keys, value, valueRange)

			Expect(valueRange).ToNot(BeNil())
		})
	})
})
