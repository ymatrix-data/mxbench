package cli_test

import (
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"

	. "github.com/ymatrix-data/mxbench/test/e2e/cli"
)

var _ = Describe("Test export config items", func() {
	It("should print help to STDOUT with stdin writer values and default generator and nil benchmark", func() {
		code, stdout, stderr, err := ExecMxbench("help --writer stdin --benchmark nil")
		Expect(code).To(BeZero())
		Expect(err).NotTo(HaveOccurred())
		Expect(stderr).To(BeEmpty())
		Expect(stdout).To(Equal(TelematicsgGeneratorStdinWriterNilBenchmarkUsage()))
	})
	It("should print help to STDOUT with stdin writer values and telematics generator and nil benchmark", func() {
		code, stdout, stderr, err := ExecMxbench("help  --generator telematics --writer stdin --benchmark nil")
		Expect(code).To(BeZero())
		Expect(err).NotTo(HaveOccurred())
		Expect(stderr).To(BeEmpty())
		Expect(stdout).To(Equal(TelematicsgGeneratorStdinWriterNilBenchmarkUsage()))
	})
	It("should print help to STDOUT with stdin writer values and nil generator and nil benchmark", func() {
		code, stdout, stderr, err := ExecMxbench("help  --generator nil --writer stdin --benchmark nil")
		Expect(code).To(BeZero())
		Expect(err).NotTo(HaveOccurred())
		Expect(stderr).To(BeEmpty())
		Expect(stdout).To(Equal(NilGeneratorStdinWriterNilBenchmarkUsage()))
	})
})
