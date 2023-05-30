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
	"path/filepath"
	"strings"
	"syscall"
)

func StartMxgate(mxgatePath, arguments string) (*exec.Cmd, io.Reader, error) {
	return StartMxgateWithContext(context.Background(), mxgatePath, arguments)
}

func StartMxgateWithContext(ctx context.Context, mxgatePath, arguments string) (*exec.Cmd, io.Reader, error) {
	mxgate, _, err := FindMxCommand(MX_GATE_CLI_BIN)
	if err != nil {
		return nil, nil, err
	}
	if len(strings.TrimSpace(mxgatePath)) > 0 && !hasMxgate(mxgatePath) {
		return nil, nil, fmt.Errorf("mxgate path %s does not exist", mxgatePath)
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

	if len(strings.TrimSpace(mxgatePath)) > 0 && !hasMxgate(mxgatePath) {
		return nil, nil, &stdoutBuf, &stderrBuf, fmt.Errorf("mxgate path %s does not exist", mxgatePath)
	}

	mxgate, _, err := FindMxCommand(MX_GATE_CLI_BIN)
	if err != nil {
		return nil, nil, &stdoutBuf, &stderrBuf, err
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

// FindMxCommand locate ABS path for a matrixdb/bin executable, such as initdb or pg_ctl.
// It will try same dir as mxctl is located, then $PATH, finally $GPHOME
func FindMxCommand(cmdName string) (cmdPath, mxDBHome string, err error) {
	var found bool

	// 1st, we find it in the same dir where mxctl resides
	// this is the safest way because PATH may not point to
	// correct $GPHOME.
	binPath, _ := os.Executable()
	binDir := filepath.Dir(binPath)
	cmdNameStr := string(cmdName)
	cmdPath = filepath.Join(binDir, cmdNameStr)
	if stat, err := os.Stat(cmdPath); err == nil {
		if m := stat.Mode(); !m.IsDir() && m.Perm()&os.FileMode(0111) != 0 {
			found = true
			mxDBHome = filepath.Dir(binDir)
		}
	}

	// 2nd, if above can't find the command, then seek in the $PATH
	// If user has sourced greenplum_path.sh, it should hit by here.
	// In a dev environment, that mxctl executable is not yet placed
	// to $GPHOME/bin, then user must have sourced the greenplum_path.sh
	if !found {
		cmdPath, err = exec.LookPath(cmdNameStr)
		if len(cmdPath) > 0 {
			mxDBHome = filepath.Dir(filepath.Dir(cmdPath))
		}
	}

	// 3rd, not found in $PATH, the only hope is user have export $GPHOME elsewhere, let's try.
	if len(mxDBHome) == 0 {
		mxDBHome = os.Getenv(ENV_KEY_MXHOME)
		if len(mxDBHome) == 0 {
			return "", "", fmt.Errorf("cannot find %s, $%s is not set", cmdName, ENV_KEY_MXHOME)
		}
		if len(cmdPath) == 0 {
			var stat os.FileInfo
			cmdPath = filepath.Join(mxDBHome, "bin", cmdNameStr)
			stat, err = os.Stat(cmdPath)
			if err != nil {
				return "", mxDBHome, fmt.Errorf("cannot find %s executable in %s", cmdName, mxDBHome)
			}
			if m := stat.Mode(); m.IsDir() || m.Perm()&os.FileMode(0111) == 0 {
				return "", mxDBHome, fmt.Errorf("invalid %s executable %s", cmdName, cmdPath)
			}
		}
	}

	return
}
