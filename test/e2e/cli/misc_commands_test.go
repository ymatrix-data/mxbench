package cli_test

import (
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"

	. "github.com/ymatrix-data/mxbench/test/e2e/cli"
)

var _ = Describe("Test misc commands", func() {
	Context("help", func() {
		It("should print help info to STDOUT", func() {
			var allArgs = []string{
				"help",
				"-h",
				"--help",
			}
			for _, args := range allArgs {
				code, stdout, stderr, err := ExecMxbench(args)
				Expect(code).To(BeZero())
				Expect(err).NotTo(HaveOccurred())
				Expect(stderr).To(BeEmpty())
				Expect(stdout).To(ContainSubstring("Usage of"))
				Expect(stdout).To(ContainSubstring("Examples"))
				Expect(stdout).To(ContainSubstring("mxbench_e2e"))
			}
		})
	})
	Context("help with env PGPORT", func() {
		It("should print help info to STDOUT", func() {
			var allArgs = []string{
				"help",
				"-h",
				"--help",
			}
			for _, args := range allArgs {
				code, stdout, stderr, err := ExecMxbenchWithEnv(args, "PGPORT=5768")
				Expect(code).To(BeZero())
				Expect(err).NotTo(HaveOccurred())
				Expect(stderr).To(BeEmpty())
				Expect(stdout).To(ContainSubstring("Usage of"))
				Expect(stdout).To(ContainSubstring("Examples"))
				Expect(stdout).To(ContainSubstring("mxbench_e2e"))
			}
		})
	})
	Context("help with env PGDATABASE", func() {
		It("should print help info to STDOUT", func() {
			var allArgs = []string{
				"help",
				"-h",
				"--help",
			}
			for _, args := range allArgs {
				code, stdout, stderr, err := ExecMxbenchWithEnv(args, "PGDATABASE=xyz")
				Expect(code).To(BeZero())
				Expect(err).NotTo(HaveOccurred())
				Expect(stderr).To(BeEmpty())
				Expect(stdout).To(ContainSubstring("Usage of"))
				Expect(stdout).To(ContainSubstring("Examples"))
				Expect(stdout).To(ContainSubstring("mxbench_e2e"))
			}
		})
	})
	Context("version", func() {
		It("should print version info to STDOUT", func() {
			code, stdout, stderr, err := ExecMxbench("--version")
			Expect(code).To(BeZero())
			Expect(err).NotTo(HaveOccurred())
			Expect(stderr).To(BeEmpty())
			Expect(stdout).To(ContainSubstring("mxbench"))
			Expect(stdout).To(ContainSubstring("(git:"))
		})
	})
})
