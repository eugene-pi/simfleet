package main

import (
	"os"

	"golang.org/x/sys/windows"
)

// enableVT turns on ANSI escape processing for a Windows console. Windows
// Terminal has it on already; the classic console host does not, and would
// print the escape codes literally.
func enableVT(f *os.File) bool {
	h := windows.Handle(f.Fd())
	var mode uint32
	if err := windows.GetConsoleMode(h, &mode); err != nil {
		return false
	}
	if mode&windows.ENABLE_VIRTUAL_TERMINAL_PROCESSING != 0 {
		return true
	}
	return windows.SetConsoleMode(h, mode|windows.ENABLE_VIRTUAL_TERMINAL_PROCESSING) == nil
}
