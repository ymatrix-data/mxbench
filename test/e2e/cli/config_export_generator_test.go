package cli_test

import (
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"github.com/pmezard/go-difflib/difflib"

	. "github.com/ymatrix-data/mxbench/test/e2e/cli"
)

var _ = Describe("Test export generator config", func() {
	It("should config nil generator", func() {
		code, stdout, stderr, err := ExecMxbench("config --generator nil")
		Expect(code).To(BeZero())
		Expect(err).NotTo(HaveOccurred())
		Expect(stderr).To(BeEmpty())
		diff, _ := difflib.GetUnifiedDiffString(difflib.UnifiedDiff{
			A: difflib.SplitLines(DefaultConfig()),
			B: difflib.SplitLines(stdout),
		})
		Expect(diff).To(ContainSubstring(`-  generator = "telematics"`))
		Expect(diff).To(ContainSubstring(`+  generator = "nil"`))
	})
	It("should config generator-disorder-ratio by args", func() {
		code, stdout, stderr, err := ExecMxbench("config --generator-disorder-ratio 10")
		Expect(code).To(BeZero())
		Expect(err).NotTo(HaveOccurred())
		Expect(stderr).To(BeEmpty())
		diff, _ := difflib.GetUnifiedDiffString(difflib.UnifiedDiff{
			A: difflib.SplitLines(DefaultConfig()),
			B: difflib.SplitLines(stdout),
		})
		Expect(diff).To(ContainSubstring(`-    # generator-disorder-ratio = 0`))
		Expect(diff).To(ContainSubstring(`+    generator-disorder-ratio = 10`))
	})
	It("should handle invalid generator-disorder-ratio", func() {
		code, stdout, stderr, err := ExecMxbench("config --generator-disorder-ratio abc")
		Expect(code).To(Equal(1))
		Expect(err).To(HaveOccurred())
		Expect(stderr).NotTo(BeEmpty())
		Expect(stdout).To(ContainSubstring("Usage of"))
		Expect(stdout).To(ContainSubstring("Examples"))
	})
	It("should config generator-empty-value-ratio by args", func() {
		code, stdout, stderr, err := ExecMxbench("config --generator-empty-value-ratio 80")
		Expect(code).To(BeZero())
		Expect(err).NotTo(HaveOccurred())
		Expect(stderr).To(BeEmpty())
		diff, _ := difflib.GetUnifiedDiffString(difflib.UnifiedDiff{
			A: difflib.SplitLines(DefaultConfig()),
			B: difflib.SplitLines(stdout),
		})
		Expect(diff).To(ContainSubstring(`-    # generator-empty-value-ratio = 90`))
		Expect(diff).To(ContainSubstring(`+    generator-empty-value-ratio = 80`))
	})
	It("should config generator-randomness by args", func() {
		code, stdout, stderr, err := ExecMxbench("config --generator-randomness S")
		Expect(code).To(BeZero())
		Expect(err).NotTo(HaveOccurred())
		Expect(stderr).To(BeEmpty())
		diff, _ := difflib.GetUnifiedDiffString(difflib.UnifiedDiff{
			A: difflib.SplitLines(DefaultConfig()),
			B: difflib.SplitLines(stdout),
		})
		Expect(diff).To(ContainSubstring(`-    # generator-randomness = "OFF"`))
		Expect(diff).To(ContainSubstring(`+    generator-randomness = "S"`))
	})
	It("should config generator-batch-size by args", func() {
		code, stdout, stderr, err := ExecMxbench("config --generator-batch-size 5")
		Expect(code).To(BeZero())
		Expect(err).NotTo(HaveOccurred())
		Expect(stderr).To(BeEmpty())
		diff, _ := difflib.GetUnifiedDiffString(difflib.UnifiedDiff{
			A: difflib.SplitLines(DefaultConfig()),
			B: difflib.SplitLines(stdout),
		})
		Expect(diff).To(ContainSubstring(`-    # generator-batch-size = 1`))
		Expect(diff).To(ContainSubstring(`+    generator-batch-size = 5`))
	})
})
