package cli_test

import (
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"github.com/pmezard/go-difflib/difflib"

	. "github.com/ymatrix-data/mxbench/test/e2e/cli"
)

var _ = Describe("Test export config items", func() {
	Context("global section", func() {
		Context("config", func() {
			It("should error when command=config and args also have --config", func() {
				code, stdout, stderr, err := ExecMxbench("config --config /tmp/abc")
				Expect(code).To(Equal(2))
				Expect(err).To(HaveOccurred())
				Expect(stderr).To(ContainSubstring("invalid usage: command config conflict with --config option"))
				Expect(stdout).To(BeEmpty())
			})
		})
		Context("log-level", func() {
			It("should config log-level to error by args", func() {
				code, stdout, stderr, err := ExecMxbench("config --log-level error")
				Expect(code).To(BeZero())
				Expect(err).NotTo(HaveOccurred())
				Expect(stderr).To(BeEmpty())
				diff, _ := difflib.GetUnifiedDiffString(difflib.UnifiedDiff{
					A: difflib.SplitLines(DefaultConfig()),
					B: difflib.SplitLines(stdout),
				})
				Expect(diff).To(ContainSubstring(`-  # log-level`))
				Expect(diff).To(ContainSubstring(`+  log-level = "error"`))
			})
			It("should config log-level to debug by args", func() {
				code, stdout, stderr, err := ExecMxbench("config --log-level debug")
				Expect(code).To(BeZero())
				Expect(err).NotTo(HaveOccurred())
				Expect(stderr).To(BeEmpty())
				diff, _ := difflib.GetUnifiedDiffString(difflib.UnifiedDiff{
					A: difflib.SplitLines(DefaultConfig()),
					B: difflib.SplitLines(stdout),
				})
				Expect(diff).To(ContainSubstring(`-  # log-level`))
				Expect(diff).To(ContainSubstring(`+  log-level = "debug"`))
			})
			It("should config log-level to verbose by args", func() {
				code, stdout, stderr, err := ExecMxbench("config --log-level verbose")
				Expect(code).To(BeZero())
				Expect(err).NotTo(HaveOccurred())
				Expect(stderr).To(BeEmpty())
				diff, _ := difflib.GetUnifiedDiffString(difflib.UnifiedDiff{
					A: difflib.SplitLines(DefaultConfig()),
					B: difflib.SplitLines(stdout),
				})
				Expect(diff).To(ContainSubstring(`-  # log-level`))
				Expect(diff).To(ContainSubstring(`+  log-level = "verbose"`))
			})
		})

	})
})
