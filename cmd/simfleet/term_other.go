//go:build !windows

package main

import "os"

// enableVT: Unix terminals interpret ANSI escapes natively.
func enableVT(*os.File) bool { return true }
