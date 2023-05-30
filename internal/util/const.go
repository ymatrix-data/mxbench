//go:build !race && !e2e
// +build !race,!e2e

package util

const (
	CLI_BIN           = "mxbench"
	MX_GATE_CLI_BIN   = "mxgated"
	GP_CONFIG_CLI_BIN = "gpconfig"
	GP_STOP_CLI_BIN   = "gpstop"
	CREATEDB_CLI_BIN  = "createdb"

	TIME_FMT         = "2006-01-02 15:04:05"
	TIME_WITH_TZ_FMT = "2006-01-02 15:04:05 -0700"
	DELIMITER        = "|"
	ENV_KEY_MXHOME   = "GPHOME"
)
