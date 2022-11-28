package util

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"os/exec"
	"strings"
	"syscall"
)

func StartMxgate(mxgatePath, arguments string) (*exec.Cmd, io.Reader, error) {
	return StartMxgateWithContext(context.Background(), mxgatePath, arguments)
}

func StartMxgateWithContext(ctx context.Context, mxgatePath, arguments string) (*exec.Cmd, io.Reader, error) {
	mxgate := MX_GATE_CLI_BIN
	if len(strings.TrimSpace(mxgatePath)) > 0 {
		if !hasMxgate(mxgatePath) {
			return nil, nil, fmt.Errorf("mxgate path %s does not exist", mxgatePath)
		}
		mxgate = mxgatePath
	}

	cmd := exec.CommandContext(ctx, mxgate, strings.Fields(arguments)...)

	outReader, err := cmd.StdoutPipe()
	if err != nil {
		return nil, nil, err
	}
	errReader, err := cmd.StderrPipe()
	if err != nil {
		return nil, nil, err
	}
	out := io.MultiReader(outReader, errReader)

	// Start mxgate as a new process group
	// so that it won't inherit mxbench's signals.
	// For example, when the user sends SIGINT to mxbench,
	// mxgate does not exit immediately. Instead, let mxbench totally control its life cycle
	// and make it quit gracefully.
	cmd.SysProcAttr = &syscall.SysProcAttr{
		Setpgid: true,
	}
	if err := cmd.Start(); err != nil {
		err = fmt.Errorf("error start mxgate: %s", err)
		return cmd, out, err
	}

	return cmd, out, nil
}

func StartMxgateStdin(mxgatePath, arguments string) (*exec.Cmd, io.WriteCloser, *bytes.Buffer, *bytes.Buffer, error) {
	return StartMxgateStdinWithContext(context.Background(), mxgatePath, arguments)
}

func StartMxgateStdinWithContext(ctx context.Context, mxgatePath, arguments string) (*exec.Cmd, io.WriteCloser, *bytes.Buffer, *bytes.Buffer, error) {
	var stdoutBuf, stderrBuf bytes.Buffer

	mxgate := MX_GATE_CLI_BIN
	if len(strings.TrimSpace(mxgatePath)) > 0 {
		if !hasMxgate(mxgatePath) {
			return nil, nil, &stdoutBuf, &stderrBuf, fmt.Errorf("mxgate path %s does not exist", mxgatePath)
		}
		mxgate = mxgatePath
	}

	cmd := exec.Command(mxgate, strings.Fields(arguments)...)

	cmd.Stdout = &stdoutBuf
	cmd.Stderr = &stderrBuf

	// Start mxgate as a new process group
	// so that it won't inherit mxbench's signals.
	// For example, when the user sends SIGINT to mxbench,
	// mxgate does not exit immediately. Instead, let mxbench totally control its life cycle
	// and make it quit gracefully.
	cmd.SysProcAttr = &syscall.SysProcAttr{
		Setpgid: true,
	}
	stdin, err := cmd.StdinPipe()
	if err != nil {
		return cmd, nil, &stdoutBuf, &stderrBuf, err
	}

	if err := cmd.Start(); err != nil {
		err = fmt.Errorf("error start mxgate: %s", err)
		return cmd, stdin, &stdoutBuf, &stderrBuf, err
	}

	return cmd, stdin, &stdoutBuf, &stderrBuf, nil
}

func hasMxgate(path string) bool {
	path = strings.TrimSpace(path)

	_, err := os.Stat(path)

	return err == nil || errors.Is(err, fs.ErrExist)
}
