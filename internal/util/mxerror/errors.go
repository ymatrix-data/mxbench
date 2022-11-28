package mxerror

import (
	"errors"
	"fmt"
	"os"
)

type ExitCode int

const (
	ExitCodeSuccess ExitCode = iota
	ExitCodeCommon
	ExitCodeIncorrectUsage
)

type MxbenchError struct {
	err      error
	ExitCode ExitCode
}

func (e *MxbenchError) Error() string {
	return e.err.Error()
}

func (e *MxbenchError) OSExit(doBeforeExit ...func()) {
	switch e.ExitCode {
	case ExitCodeCommon, ExitCodeIncorrectUsage:
		fmt.Fprintln(os.Stderr, e.err)
	}

	for _, f := range doBeforeExit {
		f()
	}
	os.Exit(int(e.ExitCode))
}

func FromError(err error) *MxbenchError {
	return &MxbenchError{
		ExitCode: ExitCodeCommon,
		err:      err,
	}
}

func Error(code ExitCode, errText string) *MxbenchError {
	return &MxbenchError{
		ExitCode: code,
		err:      errors.New(errText),
	}
}

func Errorf(code ExitCode, format string, args ...interface{}) *MxbenchError {
	return &MxbenchError{
		ExitCode: code,
		err:      fmt.Errorf(format, args...),
	}
}

func SuccessError(errMsg string) *MxbenchError {
	return Error(ExitCodeSuccess, errMsg)
}

func CommonError(errMsg string) *MxbenchError {
	return Error(ExitCodeCommon, errMsg)
}

func IncorrectUsageError(errMsg string) *MxbenchError {
	return Error(ExitCodeIncorrectUsage, errMsg)
}

func SuccessErrorf(format string, args ...interface{}) *MxbenchError {
	return Errorf(ExitCodeSuccess, format, args...)
}

func CommonErrorf(format string, args ...interface{}) *MxbenchError {
	return Errorf(ExitCodeCommon, format, args...)
}

func IncorrectUsageErrorf(format string, args ...interface{}) *MxbenchError {
	return Errorf(ExitCodeIncorrectUsage, format, args...)
}
