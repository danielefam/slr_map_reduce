package remote

import (
	"context"
	"fmt"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

const (
	DefaultParallelism = 16
	DefaultSSHTimeout  = 30 * time.Second
	DefaultSCPTimeout  = 2 * time.Minute
)

var controlPath = filepath.Join("/tmp", "slr-ssh-%C")

func NormalizeParallelism(limit, total int) int {
	if total <= 0 {
		return 1
	}
	if limit <= 0 {
		limit = DefaultParallelism
	}
	if limit > total {
		limit = total
	}
	if limit < 1 {
		return 1
	}
	return limit
}

func RunBounded[T any](items []T, limit int, fn func(T)) {
	parallelism := NormalizeParallelism(limit, len(items))
	sem := make(chan struct{}, parallelism)
	var wg sync.WaitGroup
	for _, item := range items {
		wg.Add(1)
		sem <- struct{}{}
		go func(v T) {
			defer wg.Done()
			defer func() { <-sem }()
			fn(v)
		}(item)
	}
	wg.Wait()
}

func RunSSH(host string, commands []string, timeout time.Duration) (string, error) {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	cmd := exec.CommandContext(ctx, "ssh", sshArgs(host, commands)...)
	out, err := cmd.CombinedOutput()
	return string(out), err
}

func RunSCP(src, dst string, timeout time.Duration) error {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	cmd := exec.CommandContext(ctx, "scp", scpArgs(src, dst)...)
	out, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("%w: %s", err, strings.TrimSpace(string(out)))
	}
	return nil
}

func sshArgs(host string, commands []string) []string {
	args := commonSSHArgs()
	args = append(args, host, strings.Join(commands, " && "))
	return args
}

func scpArgs(src, dst string) []string {
	args := commonSSHArgs()
	args = append(args, src, dst)
	return args
}

func commonSSHArgs() []string {
	return []string{
		"-o", "StrictHostKeyChecking=no",
		"-o", "BatchMode=yes",
		"-o", "ConnectTimeout=10",
		"-o", "ServerAliveInterval=5",
		"-o", "ServerAliveCountMax=3",
		"-o", "ControlMaster=auto",
		"-o", "ControlPersist=60s",
		"-o", "ControlPath=" + controlPath,
	}
}
