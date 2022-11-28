package cli_test

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"syscall"
	"time"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"

	"github.com/ymatrix-data/mxbench/internal/util"
	. "github.com/ymatrix-data/mxbench/test/e2e/cli"
)

var _ = Describe("Test 'run' command", func() {
	tmpCfgFile := filepath.Join(util.TempDir(), "mxbench_run_e2e.conf")
	AfterEach(func() {
		os.Remove(tmpCfgFile)
	})
	It("should shutdown on signal", func() {
		_ = os.WriteFile(tmpCfgFile, []byte(`
[benchmark]
  benchmark = "telematics"
[generator]
  generator = "telematics"
[writer]
  writer = "http"
[global]
  table-name = "st_signal_mars2"
`), 0644)
		cmd, _, _, err := StartMxbench(fmt.Sprintf("run --config %s", tmpCfgFile))

		time.Sleep(time.Second)

		Expect(err).NotTo(HaveOccurred())
		Expect(cmd).NotTo(BeNil())
		Expect(cmd.Process).NotTo(BeNil())
		Expect(cmd.ProcessState).To(BeNil())

		time.Sleep(time.Second)

		// FIXME: this signal does not work
		err = cmd.Process.Signal(syscall.SIGTERM)

		Expect(err).NotTo(HaveOccurred())

		ctx, cancel := context.WithTimeout(context.Background(), time.Second*5)
		defer cancel()

		waitChan := make(chan error)
		go func() {
			waitChan <- cmd.Wait()
		}()

		select {
		case <-ctx.Done():
		case err = <-waitChan:
		}

		Expect(ctx.Err()).NotTo(HaveOccurred())
		Expect(err).To(HaveOccurred())
		Expect(cmd.ProcessState).NotTo(BeNil())
	})
	It("run with config file has db-database", func() {
		// DebugOutput = true
		_ = os.WriteFile(tmpCfgFile, []byte(`
[database]
  db-database = 'non_existed_db_1'
[benchmark]
  benchmark = "telematics"
[generator]
  generator = "telematics"
[writer]
  writer = "http"
[global]
  table-name = "st_signal_mars2"
`), 0644)
		code, _, stderr, err := ExecMxbenchWithInjector(fmt.Sprintf("run --config %s", tmpCfgFile), "print_db")

		time.Sleep(time.Microsecond * 100)

		Expect(code).To(BeZero())
		Expect(err).NotTo(HaveOccurred())
		Expect(stderr).To(ContainSubstring("Database is non_existed_db_1"))
	})
	It("run with args --db-database and config file", func() {
		// DebugOutput = true
		_ = os.WriteFile(tmpCfgFile, []byte(`
[database]
  db-database = 'non_existed_db_1'
[benchmark]
  benchmark = "telematics"
[generator]
  generator = "telematics"
[writer]
  writer = "http"
[global]
  table-name = "st_signal_mars2"
`), 0644)
		code, _, stderr, err := ExecMxbenchWithInjector(fmt.Sprintf("run --config %s --db-database non_existed_db_3", tmpCfgFile), "print_db")

		time.Sleep(time.Microsecond * 100)

		Expect(code).To(BeZero())
		Expect(err).NotTo(HaveOccurred())
		Expect(stderr).To(ContainSubstring("Database is non_existed_db_3"))
	})
	It("run with env PGDATABASE and config file", func() {
		// DebugOutput = true
		_ = os.WriteFile(tmpCfgFile, []byte(`
[database]
  db-database = 'non_existed_db_1'
[benchmark]
  benchmark = "telematics"
[generator]
  generator = "telematics"
[writer]
  writer = "http"
[global]
  table-name = "st_signal_mars2"
`), 0644)
		code, _, stderr, err := ExecMxbenchWithEnvAndInjector(
			fmt.Sprintf(
				"run --config %s",
				tmpCfgFile,
			),
			"PGDATABASE=non_existed_db_2",
			"print_db")

		time.Sleep(time.Microsecond * 100)

		Expect(code).To(BeZero())
		Expect(err).NotTo(HaveOccurred())
		Expect(stderr).To(ContainSubstring("Database is non_existed_db_1"))
	})
	It("run with env PGDATABASE and args db-database and config file", func() {
		// DebugOutput = true
		_ = os.WriteFile(tmpCfgFile, []byte(`
[database]
  db-database = 'non_existed_db_1'
[benchmark]
  benchmark = "telematics"
[generator]
  generator = "telematics"
[writer]
  writer = "http"
[global]
  table-name = "st_signal_mars2"
`), 0644)
		code, _, stderr, err := ExecMxbenchWithEnvAndInjector(
			fmt.Sprintf(
				"run --config %s --db-database non_existed_db_3",
				tmpCfgFile,
			),
			"PGDATABASE=non_existed_db_2",
			"print_db")

		time.Sleep(time.Microsecond * 100)

		Expect(code).To(BeZero())
		Expect(err).NotTo(HaveOccurred())
		Expect(stderr).To(ContainSubstring("Database is non_existed_db_3"))
	})
})
