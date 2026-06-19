package main

import (
	"fmt"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestResolveJobImportPath(t *testing.T) {
	scriptsDir, err := resolveScriptsDir()
	if err != nil {
		t.Fatal(err)
	}

	tests := []struct {
		name    string
		jobPath string
		want    string
		wantErr string
	}{
		{
			name:    "missing job path",
			jobPath: "",
			wantErr: "job path is required",
		},
		{
			name:    "invalid job path",
			jobPath: filepath.Join(scriptsDir, "jobs", "does-not-exist"),
			wantErr: "stat job path",
		},
		{
			name:    "valid job path",
			jobPath: filepath.Join(scriptsDir, "jobs", "wordcount"),
			want:    "scripts/jobs/wordcount",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := resolveJobImportPath(scriptsDir, tt.jobPath)
			if tt.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), tt.wantErr) {
					t.Fatalf("want error containing %q, got %v", tt.wantErr, err)
				}
				return
			}
			if err != nil {
				t.Fatalf("resolveJobImportPath(%q): %v", tt.jobPath, err)
			}
			if got != tt.want {
				t.Fatalf("resolveJobImportPath(%q) = %q, want %q", tt.jobPath, got, tt.want)
			}
		})
	}
}

func TestBuildWorkerWithJob(t *testing.T) {
	scriptsDir, err := resolveScriptsDir()
	if err != nil {
		t.Fatal(err)
	}
	jobPath := filepath.Join(scriptsDir, "jobs", "wordcount")

	workerPath, err := buildWorker(jobPath)
	if err != nil {
		t.Fatalf("buildWorker(%q): %v", jobPath, err)
	}
	defer func() { _ = os.Remove(workerPath) }()

	port := freePort(t)
	cmd := exec.Command(workerPath, "-port", port)
	if err := cmd.Start(); err != nil {
		t.Fatalf("start built worker: %v", err)
	}
	defer func() {
		if cmd.Process != nil {
			_ = cmd.Process.Kill()
		}
		_ = cmd.Wait()
	}()

	url := fmt.Sprintf("http://127.0.0.1:%s/health", port)
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		resp, err := http.Get(url) //nolint:gosec
		if err == nil {
			resp.Body.Close()
			if resp.StatusCode == http.StatusOK {
				return
			}
		}
		time.Sleep(100 * time.Millisecond)
	}

	t.Fatalf("built worker never became healthy on %s", url)
}

func freePort(t *testing.T) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	_, port, err := net.SplitHostPort(ln.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	return port
}
