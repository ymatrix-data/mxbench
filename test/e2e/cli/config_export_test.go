package cli_test

import (
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"

	. "github.com/ymatrix-data/mxbench/test/e2e/cli"
)

var _ = Describe("Test export config items", func() {
	It("should print config to STDOUT with default values", func() {
		code, stdout, stderr, err := ExecMxbench("config")
		Expect(code).To(BeZero())
		Expect(err).NotTo(HaveOccurred())
		Expect(stderr).To(BeEmpty())
		Expect(stdout).To(Equal(DefaultConfig()))
	})
})
