package metadata

import (
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("Config", func() {
	It("should validate fail", func() {
		cfg := &Config{}
		err := cfg.validate()
		Expect(err).NotTo(BeNil())
	})
})
