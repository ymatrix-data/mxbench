//go:build darwin
// +build darwin

package util

// On OSX os.TempDir() returns something like /var/folders/bw/55s7r48s413gj27l6558lxg40000gp/T/
// This is annoying for development or debugging, so on OSX we force it to /tmp
func TempDir() string {
	return "/tmp"
}
