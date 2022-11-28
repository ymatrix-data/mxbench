package cli_test

import (
	"fmt"
	"os"
	"path/filepath"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"

	"github.com/ymatrix-data/mxbench/internal/util"
	. "github.com/ymatrix-data/mxbench/test/e2e/cli"
)

var _ = Describe("Test import config items", func() {
	Context("generator section", func() {
		tmpCfgFile := filepath.Join(util.TempDir(), "mxbench_generator_e2e.conf")
		AfterEach(func() {
			os.Remove(tmpCfgFile)
		})
		It("should use telematics generator defaults", func() {
			_ = os.WriteFile(tmpCfgFile, []byte(`
[generator]
  generator = "telematics"
  [generator.telematics]
`), 0644)
			code, _, stderr, err := ExecMxbenchWithInjector(fmt.Sprintf("run --config %s", tmpCfgFile), "print_generator")
			Expect(code).To(BeZero())
			Expect(err).NotTo(HaveOccurred())
			Expect(stderr).To(ContainSubstring("Generator instance is telematics"))
			Expect(stderr).To(ContainSubstring("DisorderRatio:0"))
			Expect(stderr).To(ContainSubstring("BatchSize:1"))
			Expect(stderr).To(ContainSubstring("EmptyValueRatio:90"))
			Expect(stderr).To(ContainSubstring("Randomness:OFF"))
		})
		It("should read telematics generator params", func() {
			_ = os.WriteFile(tmpCfgFile, []byte(`
[generator]
  generator = "telematics"
  [generator.telematics]
    generator-disorder-ratio = 10
    generator-empty-value-ratio = 70
    generator-randomness = "L"
	generator-batch-size = 10
`), 0644)
			code, _, stderr, err := ExecMxbenchWithInjector(fmt.Sprintf("run --config %s", tmpCfgFile), "print_generator")
			Expect(code).To(BeZero())
			Expect(err).NotTo(HaveOccurred())
			Expect(stderr).To(ContainSubstring("Generator instance is telematics"))
			Expect(stderr).To(ContainSubstring("DisorderRatio:10"))
			Expect(stderr).To(ContainSubstring("BatchSize:10"))
			Expect(stderr).To(ContainSubstring("EmptyValueRatio:70"))
			Expect(stderr).To(ContainSubstring("Randomness:L"))
		})
	})
})
