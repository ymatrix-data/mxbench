package util

import (
	"bytes"
	"os/exec"
	"path/filepath"
	"strings"

	"github.com/ymatrix-data/mxbench/internal/util/mxerror"
)

func runCmd(command string, args ...string) (*exec.Cmd, *bytes.Buffer, *bytes.Buffer, error) {
	cmdPath, err := exec.LookPath(command)
	if err != nil {
		cmdPath = filepath.Join("/tmp", command)
	}
	cmd := exec.Command(cmdPath, args...)
	var stdoutBuf, stderrBuf bytes.Buffer
	cmd.Stderr = &stderrBuf
	cmd.Stdout = &stdoutBuf
	err = cmd.Run()
	if err != nil {
		return cmd, &stdoutBuf, &stderrBuf, err
	}
	return cmd, &stdoutBuf, &stderrBuf, nil
}

func runCmdAndDealingWithError(command string, args ...string) (string, error) {
	_, stdout, stderr, err := runCmd(command, args...)
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
		return stdoutString, mxerror.CommonErrorf("err: %s\nstderr: %s\nstdout: %s", errString, stderrString, stdoutString)
	}
	return stdoutString, nil
}
