package metadata

import (
	"fmt"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("Guc", func() {
	It("GUC should implement string interface", func() {
		g := &GUC{
			Name:            "g1",
			ValueOnMaster:   "m",
			ValueOnSegments: "s",
		}

		str := g.String()

		Expect(str).To(Equal("GUC          : g1\nMaster  value: m\nSegment value: s"))
	})
	It("GUCs should implement string interface", func() {
		gs := GUCs{
			&GUC{
				Name:            "g1",
				ValueOnMaster:   "m",
				ValueOnSegments: "s",
			},
			&GUC{
				Name:            "g2",
				ValueOnMaster:   "m",
				ValueOnSegments: "s",
			},
		}

		str := gs.String()

		Expect(str).To(Equal("GUC          : g1\nMaster  value: m\nSegment value: s\n\nGUC          : g2\nMaster  value: m\nSegment value: s\n\n"))
	})

	It("get set guc command", func() {
		gs := GUCs{
			&GUC{
				Name:            "g1",
				ValueOnMaster:   "m",
				ValueOnSegments: "s",
			},
			&GUC{
				Name:            "g2",
				ValueOnMaster:   "m",
				ValueOnSegments: "s",
			},
		}

		str := gs.SetGUCsCommand()
		fmt.Println(str)

		Expect(str).To(Equal("gpconfig -c g1 -m m -v s --skipvalidation\ngpconfig -c g2 -m m -v s --skipvalidation\n"))
	})
})
