package cli_test

import (
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"

	. "github.com/ymatrix-data/mxbench/test/e2e/cli"
)

var _ = Describe("Test import config items", func() {
	Context("config section", func() {
		It("should error when config file does not exist", func() {
			code, stdout, stderr, err := ExecMxbench("run --config /tmp/not_exist")
			Expect(code).To(Equal(1))
			Expect(err).To(HaveOccurred())
			Expect(stderr).To(ContainSubstring("error reading config file"))
			Expect(stdout).To(BeEmpty())
		})
	})

})
