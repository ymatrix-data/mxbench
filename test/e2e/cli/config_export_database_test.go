package cli_test

import (
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"github.com/pmezard/go-difflib/difflib"

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
	Context("database section", func() {
		Context("db-database", func() {
			It("should config db-dabatase by env PGDATABASE", func() {
				code, stdout, stderr, err := ExecMxbenchWithEnv("config", "PGDATABASE=xyz")
				Expect(code).To(BeZero())
				Expect(err).NotTo(HaveOccurred())
				Expect(stderr).To(BeEmpty())
				diff, _ := difflib.GetUnifiedDiffString(difflib.UnifiedDiff{
					A: difflib.SplitLines(DefaultConfig()),
					B: difflib.SplitLines(stdout),
				})
				Expect(diff).To(ContainSubstring(`-  # db-database = "`))
				Expect(diff).To(ContainSubstring(`+  db-database = "xyz"`))
			})
			It("should config db-database by args", func() {
				code, stdout, stderr, err := ExecMxbench("config --db-database abc")
				Expect(code).To(BeZero())
				Expect(err).NotTo(HaveOccurred())
				Expect(stderr).To(BeEmpty())
				diff, _ := difflib.GetUnifiedDiffString(difflib.UnifiedDiff{
					A: difflib.SplitLines(DefaultConfig()),
					B: difflib.SplitLines(stdout),
				})
				Expect(diff).To(ContainSubstring(`-  # db-database = "`))
				Expect(diff).To(ContainSubstring(`+  db-database = "abc"`))
			})
			It("should config db-database by env PGDATABSE when some other value is '--db-database' ", func() {
				code, stdout, stderr, err := ExecMxbenchWithEnv("config --db-user --db-database", "PGDATABASE=abc")
				Expect(code).To(BeZero())
				Expect(err).NotTo(HaveOccurred())
				Expect(stderr).To(BeEmpty())
				diff, _ := difflib.GetUnifiedDiffString(difflib.UnifiedDiff{
					A: difflib.SplitLines(DefaultConfig()),
					B: difflib.SplitLines(stdout),
				})
				Expect(diff).To(ContainSubstring(`-  # db-database = "`))
				Expect(diff).To(ContainSubstring(`+  db-database = "abc"`))
				Expect(diff).To(ContainSubstring(`+  db-user = "--db-database"`))
			})
			It("should config db-database by args when some other args contain equal sign", func() {
				code, stdout, stderr, err := ExecMxbenchWithEnv("config --db-user=abc --db-database=xyz", "PGDATABASE=abc")
				Expect(code).To(BeZero())
				Expect(err).NotTo(HaveOccurred())
				Expect(stderr).To(BeEmpty())
				diff, _ := difflib.GetUnifiedDiffString(difflib.UnifiedDiff{
					A: difflib.SplitLines(DefaultConfig()),
					B: difflib.SplitLines(stdout),
				})
				Expect(diff).To(ContainSubstring(`-  # db-database = "`))
				Expect(diff).To(ContainSubstring(`+  db-database = "xyz"`))
				Expect(diff).To(ContainSubstring(`+  db-user = "abc"`))
			})
			It("should config db-database by args when '--db-database' preValue has prefix '-'", func() {
				code, stdout, stderr, err := ExecMxbenchWithEnv("config --db-user ---xxx --db-database xyz", "PGDATABASE=abc")
				Expect(code).To(BeZero())
				Expect(err).NotTo(HaveOccurred())
				Expect(stderr).To(BeEmpty())
				diff, _ := difflib.GetUnifiedDiffString(difflib.UnifiedDiff{
					A: difflib.SplitLines(DefaultConfig()),
					B: difflib.SplitLines(stdout),
				})
				Expect(diff).To(ContainSubstring(`-  # db-database = "`))
				Expect(diff).To(ContainSubstring(`+  db-database = "xyz"`))
				Expect(diff).To(ContainSubstring(`+  db-user = "---xxx"`))
			})
			It("should config db-database by args and ignore env", func() {
				code, stdout, stderr, err := ExecMxbenchWithEnv("config --db-database abc", "PGDATABASE=xyz")
				Expect(code).To(BeZero())
				Expect(err).NotTo(HaveOccurred())
				Expect(stderr).To(BeEmpty())
				diff, _ := difflib.GetUnifiedDiffString(difflib.UnifiedDiff{
					A: difflib.SplitLines(DefaultConfig()),
					B: difflib.SplitLines(stdout),
				})
				Expect(diff).To(ContainSubstring(`-  # db-database = "`))
				Expect(diff).To(ContainSubstring(`+  db-database = "abc"`))
			})
		})
		Context("db-master-host", func() {
			It("should config db-master-host by args", func() {
				code, stdout, stderr, err := ExecMxbench("config --db-master-host my-host.name")
				Expect(code).To(BeZero())
				Expect(err).NotTo(HaveOccurred())
				Expect(stderr).To(BeEmpty())
				diff, _ := difflib.GetUnifiedDiffString(difflib.UnifiedDiff{
					A: difflib.SplitLines(DefaultConfig()),
					B: difflib.SplitLines(stdout),
				})
				Expect(diff).To(ContainSubstring(`-  # db-master-host = `))
				Expect(diff).To(ContainSubstring(`+  db-master-host = "my-host.name"`))
			})
		})
		Context("db-master-port", func() {
			It("should replace default value by env PGPORT", func() {
				code, stdout, stderr, err := ExecMxbenchWithEnv("config", "PGPORT=5768")
				Expect(code).To(BeZero())
				Expect(err).NotTo(HaveOccurred())
				Expect(stderr).To(BeEmpty())
				diff, _ := difflib.GetUnifiedDiffString(difflib.UnifiedDiff{
					A: difflib.SplitLines(DefaultConfig()),
					B: difflib.SplitLines(stdout),
				})
				Expect(diff).NotTo(BeEmpty())
				Expect(diff).To(ContainSubstring(`-  # db-master-port = `))
				Expect(diff).To(ContainSubstring(`+  db-master-port = 5768`))
			})
			It("should tolerate wrong PGPORT fallback to default", func() {
				code, stdout, stderr, err := ExecMxbenchWithEnv("config", "PGPORT=ABC")
				Expect(code).To(BeZero())
				Expect(err).NotTo(HaveOccurred())
				Expect(stderr).To(BeEmpty())
				Expect(stdout).To(ContainSubstring(`  # db-master-port = 5432`))
			})
			It("should config db-master-port by args", func() {
				code, stdout, stderr, err := ExecMxbench("config --db-master-port 12123")
				Expect(code).To(BeZero())
				Expect(err).NotTo(HaveOccurred())
				Expect(stderr).To(BeEmpty())
				diff, _ := difflib.GetUnifiedDiffString(difflib.UnifiedDiff{
					A: difflib.SplitLines(DefaultConfig()),
					B: difflib.SplitLines(stdout),
				})
				Expect(diff).NotTo(BeEmpty())
				Expect(diff).To(ContainSubstring(`-  # db-master-port = `))
				Expect(diff).To(ContainSubstring(`+  db-master-port = 12123`))
			})
			It("should config db-database by args and ignore env", func() {
				code, stdout, stderr, err := ExecMxbenchWithEnv("config --db-master-port 12123", "PGPORT=5768")
				Expect(code).To(BeZero())
				Expect(err).NotTo(HaveOccurred())
				Expect(stderr).To(BeEmpty())
				diff, _ := difflib.GetUnifiedDiffString(difflib.UnifiedDiff{
					A: difflib.SplitLines(DefaultConfig()),
					B: difflib.SplitLines(stdout),
				})
				Expect(diff).To(ContainSubstring(`-  # db-master-port = `))
				Expect(diff).To(ContainSubstring(`+  db-master-port = 12123`))
			})
		})
		Context("db-user", func() {
			It("should replace default value by env PGUSER", func() {
				code, stdout, stderr, err := ExecMxbenchWithEnv("config", "PGUSER=stoney")
				Expect(code).To(BeZero())
				Expect(err).NotTo(HaveOccurred())
				Expect(stderr).To(BeEmpty())
				Expect(stdout).To(ContainSubstring(`  # db-user = "stoney"`))
			})
			It("should config db-user by args", func() {
				code, stdout, stderr, err := ExecMxbench("config --db-user coco")
				Expect(code).To(BeZero())
				Expect(err).NotTo(HaveOccurred())
				Expect(stderr).To(BeEmpty())
				diff, _ := difflib.GetUnifiedDiffString(difflib.UnifiedDiff{
					A: difflib.SplitLines(DefaultConfig()),
					B: difflib.SplitLines(stdout),
				})
				Expect(diff).To(ContainSubstring(`-  # db-user = "`))
				Expect(diff).To(ContainSubstring(`+  db-user = "coco"`))
			})
			It("should config db-user by args and ignore env", func() {
				code, stdout, stderr, err := ExecMxbenchWithEnv("config --db-user coco", "PGUSER=stoney")
				Expect(code).To(BeZero())
				Expect(err).NotTo(HaveOccurred())
				Expect(stderr).To(BeEmpty())
				diff, _ := difflib.GetUnifiedDiffString(difflib.UnifiedDiff{
					A: difflib.SplitLines(DefaultConfig()),
					B: difflib.SplitLines(stdout),
				})
				Expect(diff).To(ContainSubstring(`-  # db-user = "`))
				Expect(diff).To(ContainSubstring(`+  db-user = "coco"`))
			})
		})
		Context("db-password", func() {
			It("should NOT replace default value by env PGPASSWORD", func() {
				// security consideration, don't disclose password to config
				code, stdout, stderr, err := ExecMxbenchWithEnv("config", "PGPASSWORD=changeme")
				Expect(code).To(BeZero())
				Expect(err).NotTo(HaveOccurred())
				Expect(stderr).To(BeEmpty())
				Expect(stdout).To(ContainSubstring(`  # db-password = ""`))
			})
			It("should config db-password by args", func() {
				code, stdout, stderr, err := ExecMxbench("config --db-password changeit")
				Expect(code).To(BeZero())
				Expect(err).NotTo(HaveOccurred())
				Expect(stderr).To(BeEmpty())
				diff, _ := difflib.GetUnifiedDiffString(difflib.UnifiedDiff{
					A: difflib.SplitLines(DefaultConfig()),
					B: difflib.SplitLines(stdout),
				})
				Expect(diff).To(ContainSubstring(`-  # db-password = ""`))
				Expect(diff).To(ContainSubstring(`+  db-password = "changeit"`))
			})
		})
	})
})
