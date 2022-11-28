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

var _ = Describe("Test import writer config", func() {
	Context("cli and conf priority", func() {
		tmpCfgFile := filepath.Join(util.TempDir(), "mxbench_http_e2e.conf")
		AfterEach(func() {
			os.Remove(tmpCfgFile)
		})
		It("should use CLI --writer-parallel override config file", func() {
			_ = os.WriteFile(tmpCfgFile, []byte(`
[writer]
  writer = "http"
  [writer.http]
    ## The parallel of http writer
    writer-parallel = 16
			`), 0644)
			code, _, stderr, err := ExecMxbenchWithInjector(fmt.Sprintf("run --config %s --writer-parallel 32", tmpCfgFile), "print_http_config")
			Expect(stderr).To(ContainSubstring("Parallel:32"))
			Expect(code).To(BeZero())
			Expect(err).NotTo(HaveOccurred())
		})
		It("should use CLI --writer http --writer-parallel override config file", func() {
			_ = os.WriteFile(tmpCfgFile, []byte(`
[writer]
  writer = "http"
  [writer.http]
  ## The parallel of http writer
  writer-parallel = 16
			`), 0644)
			code, _, stderr, err := ExecMxbenchWithInjector(fmt.Sprintf("run --config %s --writer http --writer-parallel 8", tmpCfgFile), "print_http_config")
			Expect(stderr).To(ContainSubstring("Parallel:8"))
			Expect(code).To(BeZero())
			Expect(err).NotTo(HaveOccurred())
		})
		It("should use CLI --writer-use-gzip override config file", func() {
			_ = os.WriteFile(tmpCfgFile, []byte(`
[writer]
  writer = "http"
  [writer.http]
  ## The parallel of http writer
  writer-parallel = 16

  ## use gzip for http writer
  writer-use-gzip = false
			`), 0644)
			code, _, stderr, err := ExecMxbenchWithInjector(fmt.Sprintf("run --config %s --writer-use-gzip true", tmpCfgFile), "print_http_config")
			Expect(stderr).To(ContainSubstring("UseGzip:true"))
			Expect(code).To(BeZero())
			Expect(err).NotTo(HaveOccurred())
		})
		It("should use CLI --writer http --writer-use-gzip override config file", func() {
			_ = os.WriteFile(tmpCfgFile, []byte(`
[writer]
  writer = "http"
  [writer.http]
  ## The parallel of http writer
  writer-parallel = 16

  ## use gzip for http writer
  writer-use-gzip = false
			`), 0644)
			code, _, stderr, err := ExecMxbenchWithInjector(fmt.Sprintf("run --config %s --writer http --writer-use-gzip true", tmpCfgFile), "print_http_config")
			Expect(stderr).To(ContainSubstring("UseGzip:true"))
			Expect(code).To(BeZero())
			Expect(err).NotTo(HaveOccurred())
		})
	})

})
