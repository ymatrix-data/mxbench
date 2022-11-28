package metadata

import (
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"

	"github.com/ymatrix-data/mxbench/pkg/mxmock"
)

var _ = Describe("Column", func() {
	It("test columns to sql string", func() {
		cls := Columns{
			&mxmock.Column{
				Name:     "c1",
				TypeName: "float64",
			},
			&mxmock.Column{
				Name:     "c2",
				TypeName: "float64",
			},
			&mxmock.Column{
				Name:     "c3",
				TypeName: "float64",
			},
		}
		Expect(cls.ToSQLStr()).To(Equal("\tc1 float64\n  , c2 float64\n  , c3 float64"))
	})
})
