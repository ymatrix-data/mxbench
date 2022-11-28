//go:build e2e
// +build e2e

package util

import "github.com/spf13/pflag"

/* THIS FILE IS ONLY COMPILED IN END-TO-END TEST */

const (
	CLI_BIN           = "mxbench_e2e"
	MX_GATE_CLI_BIN   = "mxgated"
	GP_CONFIG_CLI_BIN = "gpconfig"
	GP_STOP_CLI_BIN   = "gpstop"
	CREATEDB_CLI_BIN  = "createdb"

	TIME_FMT         = "2006-01-02 15:04:05"
	TIME_WITH_TZ_FMT = "2006-01-02 15:04:05 -0700"
	DELIMITER        = "|"
)

func init() {
	var injectorName string

	// Only on e2e build, accept the injector flag
	// Accept and pass --injector to mxbench
	pflag.CommandLine.StringVar(&injectorName, "injector", "", "The name of injector")
}
