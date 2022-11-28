package cli

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"strings"
	"time"

	"github.com/ymatrix-data/mxbench/internal/util"
)

type key int

const (
	keyEnv key = iota
	keyInjector
	keyTimoutSeconds
)

var (
	DebugOutput = false
	errTimeout  = errors.New("timeout")
)

func ExecMxbench(arguments string) (code int, outMsg, errMsg string, err error) {
	return ExecMxbenchWithContext(context.Background(), arguments)
}

func ExecMxbenchWithInjector(arguments, injector string) (code int, outMsg string, errMsg string, err error) {
	ctx := context.WithValue(context.Background(), keyInjector, injector)
	return ExecMxbenchWithTimeout(ctx, arguments, 5)
}

func ExecMxbenchWithEnv(arguments, env string) (code int, outMsg, errMsg string, err error) {
	ctx := context.WithValue(context.Background(), keyEnv, env)
	return ExecMxbenchWithContext(ctx, arguments)
}

func ExecMxbenchWithEnvAndInjector(arguments, env, injector string) (code int, outMsg, errMsg string, err error) {
	ctx := context.WithValue(context.WithValue(context.Background(), keyEnv, env), keyInjector, injector)
	return ExecMxbenchWithContext(ctx, arguments)
}

func ExecMxbenchWithTimeout(ctx context.Context, arguments string, timeoutSeconds int) (code int, outMsg string, errMsg string, err error) {
	ctx = context.WithValue(ctx, keyTimoutSeconds, timeoutSeconds)
	return ExecMxbenchWithContext(ctx, arguments)
}

func ExecMxbenchWithContext(ctx context.Context, arguments string) (code int, outMsg string, errMsg string, err error) {
	var stdoutBuf, stderrBuf bytes.Buffer
	code = -1 // timeout

	iEnv := ctx.Value(keyEnv)
	iTimeoutSeconds := ctx.Value(keyTimoutSeconds)
	iInjector := ctx.Value(keyInjector)
	if iInjector != nil && iInjector.(string) != "" {
		arguments += " --injector " + iInjector.(string)
	}

	cmd := exec.Command(util.CLI_BIN, strings.Fields(arguments)...)
	if iEnv != nil {
		cmd.Env = strings.Fields(iEnv.(string))
	}

	if DebugOutput {
		cmd.Stdout = io.MultiWriter(os.Stdout, &stdoutBuf)
		cmd.Stderr = io.MultiWriter(os.Stderr, &stderrBuf)
	} else {
		cmd.Stdout = &stdoutBuf
		cmd.Stderr = &stderrBuf
	}

	if err = cmd.Start(); err != nil {
		err = fmt.Errorf("error start mxbench: %s", err)
		return
	}

	if iTimeoutSeconds == nil || iTimeoutSeconds.(int) == 0 {
		err = cmd.Wait()
		outMsg = stdoutBuf.String()
		errMsg = stderrBuf.String()
		code = cmd.ProcessState.ExitCode()
		if err != nil {
			err = fmt.Errorf("mxbench returns error: %s", err)
		}
		return
	}

	waitChan := make(chan error)
	timeout := time.NewTimer(time.Second * time.Duration(iTimeoutSeconds.(int)))
	go func() {
		waitChan <- cmd.Wait()
	}()

	select {
	case err = <-waitChan:
		outMsg = stdoutBuf.String()
		errMsg = stderrBuf.String()
		code = cmd.ProcessState.ExitCode()
		return
	case <-timeout.C:
		outMsg = stdoutBuf.String()
		errMsg = stderrBuf.String()
		err = errTimeout
	}

	return
}

func StartMxbench(arguments string) (*exec.Cmd, *bytes.Buffer, *bytes.Buffer, error) {
	return StartMxbenchWithContext(context.Background(), arguments)
}

func StartMxbenchWithContext(ctx context.Context, arguments string) (*exec.Cmd, *bytes.Buffer, *bytes.Buffer, error) {
	var stdoutBuf, stderrBuf bytes.Buffer

	iEnv := ctx.Value(keyEnv)
	iInjector := ctx.Value(keyInjector)
	if iInjector != nil && iInjector.(string) != "" {
		arguments += " --injector " + iInjector.(string)
	}

	cmd := exec.Command(util.CLI_BIN, strings.Fields(arguments)...)
	if iEnv != nil {
		cmd.Env = strings.Fields(iEnv.(string))
	}

	if DebugOutput {
		cmd.Stdout = io.MultiWriter(os.Stdout, &stdoutBuf)
		cmd.Stderr = io.MultiWriter(os.Stderr, &stderrBuf)
	} else {
		cmd.Stdout = &stdoutBuf
		cmd.Stderr = &stderrBuf
	}

	if err := cmd.Start(); err != nil {
		err = fmt.Errorf("error start mxbench: %s", err)
		return cmd, &stdoutBuf, &stderrBuf, err
	}

	return cmd, &stdoutBuf, &stderrBuf, nil
}
