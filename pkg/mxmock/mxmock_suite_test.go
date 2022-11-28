package mxmock

import (
	"testing"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

func TestMxmock(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Mxmock Suite")
}
