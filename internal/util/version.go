package util

import (
	"fmt"
)

var (
	VersionStr string
	BranchStr  string
	CommitStr  string
)

func GetMxbenchVersion() string {
	return fmt.Sprintf("%s (git: %s %s)", VersionStr, BranchStr, CommitStr)
}

func GetVersionStr() string {
	return VersionStr
}

func PrintLogo(license string) {
	// Generated with http://patorjk.com/software/taag/#p=display&f=Standard&t=mxbench
	fmt.Println("******************************************************")
	fmt.Println("                  _                     _     ")
	fmt.Println("   _ __ ___ __  _| |__   ___ _ __   ___| |__  ")
	fmt.Println("  | '_ ` _ \\\\ \\/ / '_ \\ / _ \\ '_ \\ / __| '_ \\ ")
	fmt.Println("  | | | | | |>  <| |_) |  __/ | | | (__| | | |")
	fmt.Println("  |_| |_| |_/_/\\_\\_.__/ \\___|_| |_|\\___|_| |_|")
	fmt.Println()
	fmt.Println("  Version:", GetMxbenchVersion())
	fmt.Println("  Your Copy is Licensed to:", license)
	fmt.Println("******************************************************")
}
