//go:build !darwin
// +build !darwin

package util

import "os"

func TempDir() string {
	return os.TempDir()
}
