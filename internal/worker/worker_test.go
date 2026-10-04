package worker

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func localConfig() Config {
	return Config{Action: "stage", Backend: "local", PlanUID: "plan-uid", Generation: "w000000", RelativePath: "windowflow/plan-uid/w000000", Source: "window", ExpectedBytes: 5}
}
func put(t *testing.T, path, content string) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte(content), 0644); err != nil {
		t.Fatal(err)
	}
}

func TestLocalStageRetryCleanAndSourcePreservation(t *testing.T) {
	cache, source := t.TempDir(), t.TempDir()
	cfg := localConfig()
	put(t, filepath.Join(source, "window/sub/video.bin"), "video")
	put(t, filepath.Join(cache, "unrelated.txt"), "keep")
	for range 2 {
		if err := Run(context.Background(), cfg, cache, source); err != nil {
			t.Fatal(err)
		}
	}
	p := filepath.Join(cache, cfg.RelativePath, "sub/video.bin")
	if b, err := os.ReadFile(p); err != nil || string(b) != "video" {
		t.Fatal("copy mismatch", err)
	}
	cfg.Action = "clean"
	for range 2 {
		if err := Run(context.Background(), cfg, cache, source); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := os.Stat(p); !os.IsNotExist(err) {
		t.Fatal("generation remains")
	}
	for _, p := range []string{filepath.Join(source, "window/sub/video.bin"), filepath.Join(cache, "unrelated.txt")} {
		if _, err := os.Stat(p); err != nil {
			t.Fatal("unrelated or source removed", err)
		}
	}
}

func TestWorkerRejectsEscapesWrongBytesAndUnownedCleanup(t *testing.T) {
	for _, field := range []string{"source", "target", "bytes"} {
		t.Run(field, func(t *testing.T) {
			cache, source := t.TempDir(), t.TempDir()
			put(t, filepath.Join(source, "window/video.bin"), "video")
			cfg := localConfig()
			switch field {
			case "source":
				cfg.Source = "../other"
			case "target":
				cfg.RelativePath = "windowflow/another/w000000"
			case "bytes":
				cfg.ExpectedBytes = 4
			}
			if err := Run(context.Background(), cfg, cache, source); err == nil {
				t.Fatal("unsafe request accepted")
			}
		})
	}
	cache, source := t.TempDir(), t.TempDir()
	cfg := localConfig()
	cfg.Action = "clean"
	file := filepath.Join(cache, cfg.RelativePath, "keep.txt")
	put(t, file, "keep")
	if err := Run(context.Background(), cfg, cache, source); err == nil {
		t.Fatal("unowned directory deleted")
	}
	if _, err := os.Stat(file); err != nil {
		t.Fatal("unowned data touched")
	}
}

func TestSymlinksAndChangedOwnerRefuseDestruction(t *testing.T) {
	cache, source, outside := t.TempDir(), t.TempDir(), t.TempDir()
	cfg := localConfig()
	put(t, filepath.Join(source, "window/video.bin"), "video")
	put(t, filepath.Join(outside, "precious"), "keep")
	if err := os.Symlink(outside, filepath.Join(source, "window/link")); err != nil {
		t.Fatal(err)
	}
	if err := Run(context.Background(), cfg, cache, source); err == nil {
		t.Fatal("symlink source accepted")
	}
	if err := os.Remove(filepath.Join(source, "window/link")); err != nil {
		t.Fatal(err)
	}
	if err := Run(context.Background(), cfg, cache, source); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(outside, filepath.Join(cache, cfg.RelativePath, "link")); err != nil {
		t.Fatal(err)
	}
	cfg.Action = "clean"
	if err := Run(context.Background(), cfg, cache, source); err == nil {
		t.Fatal("symlink destination accepted")
	}
	if _, err := os.Stat(filepath.Join(outside, "precious")); err != nil {
		t.Fatal("symlink followed")
	}
	if err := os.Remove(filepath.Join(cache, cfg.RelativePath, "link")); err != nil {
		t.Fatal(err)
	}
	put(t, filepath.Join(cache, cfg.RelativePath, ownerName), `{"version":1,"planUID":"other"}`)
	if err := Run(context.Background(), cfg, cache, source); err == nil {
		t.Fatal("wrong owner accepted")
	}
}

func TestCanceledWorkerDoesNotWrite(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	cache := t.TempDir()
	if err := Run(ctx, localConfig(), cache, t.TempDir()); err != context.Canceled {
		t.Fatal(err)
	}
	entries, _ := os.ReadDir(cache)
	if len(entries) != 0 {
		t.Fatal("canceled worker wrote files")
	}
}

func cloudConfig() Config {
	c := localConfig()
	c.Backend = "cpfs-dataflow"
	c.DataFlow = &DataFlowConfig{Region: "cn-hangzhou", FileSystemID: "bmcpfs-test", DataFlowID: "df-test", FileSystemPath: "/datasets/", PVCPath: "/datasets/cache/"}
	return c
}

func TestDataFlowMappingAndValidation(t *testing.T) {
	c := cloudConfig()
	got, err := dataFlowDestination(c)
	if err != nil || got != "/cache/windowflow/plan-uid/w000000/" {
		t.Fatal(got, err)
	}
	for _, p := range []string{"/datasets-other/", "/datasets/../outside/", "relative"} {
		c.DataFlow.PVCPath = p
		if Validate(c) == nil {
			t.Fatal("bad mapping accepted", p)
		}
	}
}

// The fake verifies the documented request contract and import completion
// handling; it does not establish how the real provider maps OSS objects.
func TestCloudTaskResumeAndPartialFailure(t *testing.T) {
	for _, mode := range []string{"success", "partial", "report", "unknown", "wrong-task", "ambiguous"} {
		t.Run(mode, func(t *testing.T) {
			cfg := cloudConfig()
			dir := t.TempDir()
			parent, err := os.OpenRoot(dir)
			if err != nil {
				t.Fatal(err)
			}
			defer parent.Close()
			dst, err := ownedDirectory(parent, cfg, true)
			if err != nil {
				t.Fatal(err)
			}
			defer dst.Close()
			put(t, filepath.Join(dir, cfg.Generation, "video.bin"), "video")
			var creates atomic.Int32
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if err := r.ParseForm(); err != nil {
					t.Error(err)
					w.WriteHeader(400)
					return
				}
				v := r.PostForm
				signature := v.Get("Signature")
				v.Del("Signature")
				if r.Method != "POST" || signature != rpcSignature(v, "secret") || v.Get("SecurityToken") != "temporary" {
					t.Error("invalid signing or auth contract")
				}
				w.Header().Set("Content-Type", "application/json")
				if v.Get("Action") == "CreateDataFlowTask" {
					creates.Add(1)
					if v.Get("Directory") != "/window/" || v.Get("DstDirectory") != "/cache/windowflow/plan-uid/w000000/" || v.Get("TaskAction") != "Import" || v.Get("DataType") != "MetaAndData" || v.Get("ClientToken") != configDigest(cfg) || v.Get("ConflictPolicy") != "SKIP_THE_FILE" {
						t.Error("invalid import request")
					}
					if mode == "ambiguous" {
						w.WriteHeader(500)
						fmt.Fprint(w, `{"Code":"InternalError"}`)
						return
					}
					fmt.Fprint(w, `{"TaskId":"task-test"}`)
					return
				}
				if v.Get("Action") != "DescribeDataFlowTasks" || v.Get("Filters.1.Key") != "TaskIds" || v.Get("Filters.1.Value") != "task-test" || v.Get("WithReports") != "true" {
					t.Error("invalid describe request")
				}
				task := map[string]any{"TaskId": "task-test", "FilesystemId": "bmcpfs-test", "DataFlowId": "df-test", "TaskAction": "Import", "DataType": "MetaAndData", "Directory": "/window/", "DstDirectory": "/cache/windowflow/plan-uid/w000000/", "Status": "Completed", "ProgressStats": map[string]int64{"FilesTotal": 1, "FilesDone": 1, "BytesTotal": 5, "BytesDone": 5}}
				switch mode {
				case "partial":
					task["ProgressStats"].(map[string]int64)["BytesDone"] = 4
				case "report":
					task["Reports"] = map[string]any{"Report": []map[string]string{{"Name": "FailedFilesReport", "Path": "https://secret-signed-url.invalid"}}}
				case "unknown":
					task["Status"] = "Surprise"
				case "wrong-task":
					task["TaskId"] = "task-other"
				}
				_ = json.NewEncoder(w).Encode(map[string]any{"TaskInfo": map[string]any{"Task": []any{task}}})
			}))
			defer server.Close()
			c := &dataFlowClient{endpoint: server.URL, region: "cn-hangzhou", keyID: "id", secret: "secret", token: "temporary", http: server.Client(), poll: time.Millisecond}
			for range 2 {
				err := stageDataFlow(context.Background(), cfg, parent, dst, c)
				if (err == nil) != (mode == "success") {
					t.Fatalf("mode %s: %v", mode, err)
				}
				if err != nil && strings.Contains(err.Error(), "secret-signed-url") {
					t.Fatal("report credentials exposed")
				}
			}
			if creates.Load() != 1 {
				t.Fatal("retry created another cloud task")
			}
		})
	}
}

func TestRPCEncoding(t *testing.T) {
	if got := rpcEncode("a b+*/~中文"); got != "a%20b%2B%2A%2F~%E4%B8%AD%E6%96%87" {
		t.Fatal(got)
	}
}
