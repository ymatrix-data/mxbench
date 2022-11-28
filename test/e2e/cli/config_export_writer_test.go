package cli_test

import (
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"github.com/pmezard/go-difflib/difflib"

	. "github.com/ymatrix-data/mxbench/test/e2e/cli"
)

var _ = Describe("Test export writer config", func() {
	It("should config writer by args", func() {
		code, stdout, stderr, err := ExecMxbench("config --writer no-such-writer")
		Expect(code).To(BeZero())
		Expect(err).NotTo(HaveOccurred())
		Expect(stderr).To(BeEmpty())
		diff, _ := difflib.GetUnifiedDiffString(difflib.UnifiedDiff{
			A: difflib.SplitLines(DefaultConfig()),
			B: difflib.SplitLines(stdout),
		})
		Expect(diff).To(ContainSubstring(`-  writer = "http"`))
		Expect(diff).To(ContainSubstring(`+  writer = "no-such-writer"`))
	})
	Context("http writer config", func() {
		It("should use http as default writer", func() {
			code, stdout, stderr, err := ExecMxbench("config")
			Expect(code).To(BeZero())
			Expect(err).NotTo(HaveOccurred())
			Expect(stderr).To(BeEmpty())
			Expect(stdout).To(ContainSubstring(`  writer = "http"`))
			Expect(stdout).To(ContainSubstring(`# writer-parallel = 8`))
		})
	})
})
