package telematics_test

import (
	"testing"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

func TestTelematicsBenchmark(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Telematics Benchmark Suite")
}
