package cli_test

import (
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"github.com/pmezard/go-difflib/difflib"

	. "github.com/ymatrix-data/mxbench/test/e2e/cli"
)

var _ = Describe("Test export benchmark config", func() {
	It("should config nil benchmark", func() {
		code, stdout, stderr, err := ExecMxbench("config --benchmark nil")
		Expect(code).To(BeZero())
		Expect(err).NotTo(HaveOccurred())
		Expect(stderr).To(BeEmpty())
		diff, _ := difflib.GetUnifiedDiffString(difflib.UnifiedDiff{
			A: difflib.SplitLines(DefaultConfig()),
			B: difflib.SplitLines(stdout),
		})
		Expect(diff).To(ContainSubstring(`-  benchmark = "telematics"`))
		Expect(diff).To(ContainSubstring(`+  benchmark = "nil"`))
	})
	It("should config parallel by args", func() {
		code, stdout, stderr, err := ExecMxbench("config --writer-parallel 400")
		Expect(code).To(BeZero())
		Expect(err).NotTo(HaveOccurred())
		Expect(stderr).To(BeEmpty())
		diff, _ := difflib.GetUnifiedDiffString(difflib.UnifiedDiff{
			A: difflib.SplitLines(DefaultConfig()),
			B: difflib.SplitLines(stdout),
		})
		Expect(diff).To(ContainSubstring(`-    # writer-parallel = 8`))
		Expect(diff).To(ContainSubstring(`+    writer-parallel = 400`))
	})
	It("should handle invalid parallel", func() {
		code, stdout, stderr, err := ExecMxbench("config --writer-parallel abc")
		Expect(code).To(Equal(1))
		Expect(err).To(HaveOccurred())
		Expect(stderr).NotTo(BeEmpty())
		Expect(stdout).To(ContainSubstring("Usage of"))
		Expect(stdout).To(ContainSubstring("Examples"))
	})
})
