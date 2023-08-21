package util

import (
	"strconv"
	"strings"

	"github.com/ymatrix-data/mxbench/internal/util/mxerror"
)

func (params *DBConnParams) GetCreateDBParams() []string {
	//   Connection options:
	// 	-h, --host=HOSTNAME          database server host or socket directory
	// 	-p, --port=PORT              database server port
	// 	-U, --username=USERNAME      user name to connect as
	// 	-w, --no-password            never prompt for password
	// 	-W, --password               force password prompt

	pwdStr := "--no-password"
	if params.Password != "" {
		pwdStr = "--password=" + params.Password
	}

	return []string{
		"--host=" + params.MasterHost,
		"--port=" + strconv.Itoa(params.MasterPort),
		"--username=" + params.User,
		pwdStr, params.Database}
}

func CreateDBIfNotExists(params DBConnParams) error {
	_, stdout, stderr, err := runCmd(CREATEDB_CLI_BIN, params.GetCreateDBParams()...)
	var errString, stderrString, stdoutString string
	if err != nil {
		errString = err.Error()
	}
	if stdout != nil {
		stdoutString = stdout.String()
	}
	if stderr != nil {
		stderrString = stderr.String()
	}
	if strings.Contains(stderrString, "already exists") {
		return nil
	}
	if err != nil || len(stderrString) > 0 || strings.Contains(stdoutString, "ERROR") {
		return mxerror.CommonErrorf("err: %s\nstderr: %s\nstdout: %s", errString, stderrString, stdoutString)
	}
	return nil
}

func ExecuteDBCmd(cmd string, params DBConnParams) error {
	_, stdout, stderr, err := runCmd(cmd, params.GetCreateDBParams()...)
	var errString, stderrString, stdoutString string
	if err != nil {
		errString = err.Error()
	}
	if stdout != nil {
		stdoutString = stdout.String()
	}
	if stderr != nil {
		stderrString = stderr.String()
	}

	if err != nil || len(stderrString) > 0 || strings.Contains(stdoutString, "ERROR") {
		return mxerror.CommonErrorf("err: %s\nstderr: %s\nstdout: %s", errString, stderrString, stdoutString)
	}
	return nil
}
