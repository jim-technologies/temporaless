//go:build unix

package main

import "syscall"

func setPgid() *syscall.SysProcAttr {
	return &syscall.SysProcAttr{Setpgid: true}
}
