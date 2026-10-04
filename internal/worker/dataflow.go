// SPDX-License-Identifier: Apache-2.0
package worker

import (
	"context"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha1" // Alibaba Cloud RPC v1 requires HMAC-SHA1.
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"log/slog"
	"net/http"
	"net/url"
	"os"
	"strings"
	"syscall"
	"time"
)

// This adapter uses the documented NAS 2017-06-26 RPC API. It is experimental:
// fake-server tests do not verify CPFS directory mapping or cloud permissions.
type dataFlowClient struct {
	endpoint, region, keyID, secret, token string
	http                                   *http.Client
	poll                                   time.Duration
}

func randomID() string {
	b := make([]byte, 16)
	_, _ = rand.Read(b) // crypto/rand.Read never returns an error on supported Go.
	return hex.EncodeToString(b)
}

func newDataFlowClient(cfg *DataFlowConfig) (*dataFlowClient, error) {
	id, secret := os.Getenv("ALIBABA_CLOUD_ACCESS_KEY_ID"), os.Getenv("ALIBABA_CLOUD_ACCESS_KEY_SECRET")
	if id == "" || secret == "" {
		return nil, fmt.Errorf("cloud access key environment variables are required")
	}
	return &dataFlowClient{
		endpoint: "https://nas." + cfg.Region + ".aliyuncs.com/", region: cfg.Region,
		keyID: id, secret: secret, token: os.Getenv("ALIBABA_CLOUD_SECURITY_TOKEN"),
		http: &http.Client{Timeout: 60 * time.Second, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }},
		poll: 10 * time.Second,
	}, nil
}

func rpcEncode(s string) string          { return strings.ReplaceAll(url.QueryEscape(s), "+", "%20") }
func canonicalQuery(v url.Values) string { return strings.ReplaceAll(v.Encode(), "+", "%20") }
func rpcSignature(v url.Values, secret string) string {
	mac := hmac.New(sha1.New, []byte(secret+"&"))
	_, _ = mac.Write([]byte("POST&%2F&" + rpcEncode(canonicalQuery(v))))
	return base64.StdEncoding.EncodeToString(mac.Sum(nil))
}

func (c *dataFlowClient) call(ctx context.Context, action string, args url.Values, out any) error {
	v := url.Values{}
	for k, values := range args {
		v[k] = append([]string(nil), values...)
	}
	for k, value := range map[string]string{
		"Action": action, "Version": "2017-06-26", "Format": "JSON", "RegionId": c.region,
		"AccessKeyId": c.keyID, "SignatureMethod": "HMAC-SHA1", "SignatureVersion": "1.0",
		"SignatureNonce": randomID(), "Timestamp": time.Now().UTC().Format("2006-01-02T15:04:05Z"),
	} {
		v.Set(k, value)
	}
	if c.token != "" {
		v.Set("SecurityToken", c.token)
	}
	v.Set("Signature", rpcSignature(v, c.secret))
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.endpoint, strings.NewReader(v.Encode()))
	if err != nil {
		return fmt.Errorf("create NAS request: invalid endpoint")
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	resp, err := c.http.Do(req)
	if err != nil {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		return fmt.Errorf("NAS %s transport failed; task outcome may be unknown", action)
	}
	defer resp.Body.Close()
	b, err := io.ReadAll(io.LimitReader(resp.Body, 2*1024*1024+1))
	if err != nil || len(b) > 2*1024*1024 {
		return fmt.Errorf("NAS %s response unreadable or too large", action)
	}
	var envelope struct{ Code, RequestID string }
	if err := json.Unmarshal(b, &envelope); err != nil {
		return fmt.Errorf("NAS %s returned invalid JSON (HTTP %d)", action, resp.StatusCode)
	}
	if resp.StatusCode != http.StatusOK || envelope.Code != "" {
		// Do not log raw responses, messages, report URLs, or signed requests.
		return fmt.Errorf("NAS %s rejected (HTTP %d, code=%q, requestId=%q)", action, resp.StatusCode, envelope.Code, envelope.RequestID)
	}
	if err := json.Unmarshal(b, out); err != nil {
		return fmt.Errorf("NAS %s returned unexpected response schema", action)
	}
	return nil
}

type taskReceipt struct {
	Digest string `json:"digest"`
	TaskID string `json:"taskId"`
}

// Persist an intent before calling the cloud, then the returned task ID. A crash
// in between is deliberately ambiguous and requires manual reconciliation; a
// later Pod must never create a second task while the first may still write.
func (c *dataFlowClient) task(ctx context.Context, cfg Config, parent *os.Root) (string, error) {
	name := "." + cfg.Generation + ".dataflow-task.json"
	info, err := parent.Lstat(name)
	if err == nil {
		if !info.Mode().IsRegular() || info.Size() > maxMarkerBytes {
			return "", fmt.Errorf("invalid cloud task receipt")
		}
		f, err := parent.OpenFile(name, os.O_RDONLY|syscall.O_NOFOLLOW, 0)
		if err != nil {
			return "", err
		}
		defer f.Close()
		var receipt taskReceipt
		d := json.NewDecoder(io.LimitReader(f, maxMarkerBytes+1))
		d.DisallowUnknownFields()
		if err := d.Decode(&receipt); err != nil {
			return "", fmt.Errorf("invalid cloud task receipt")
		}
		if receipt.Digest != configDigest(cfg) || receipt.TaskID == "" || !componentPattern.MatchString(receipt.TaskID) {
			return "", fmt.Errorf("ambiguous or mismatched cloud task receipt; inspect provider before recovery")
		}
		return receipt.TaskID, nil
	}
	if !errors.Is(err, fs.ErrNotExist) {
		return "", err
	}
	receipt := taskReceipt{Digest: configDigest(cfg)}
	f, err := parent.OpenFile(name, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
	if err != nil {
		return "", err
	}
	err = json.NewEncoder(f).Encode(receipt)
	if err == nil {
		err = f.Sync()
	}
	closeErr := f.Close()
	if err == nil {
		err = closeErr
	}
	if err != nil {
		return "", err
	}
	dest, err := dataFlowDestination(cfg)
	if err != nil {
		return "", err
	}
	args := url.Values{
		"FileSystemId": {cfg.DataFlow.FileSystemID}, "DataFlowId": {cfg.DataFlow.DataFlowID},
		"ClientToken": {receipt.Digest}, "TaskAction": {"Import"}, "DataType": {"MetaAndData"},
		"Directory": {"/" + cfg.Source + "/"}, "DstDirectory": {dest}, "CreateDirIfNotExist": {"true"},
		"ConflictPolicy": {"SKIP_THE_FILE"},
	}
	var result struct{ TaskID string }
	if err := c.call(ctx, "CreateDataFlowTask", args, &result); err != nil {
		return "", err
	}
	if !componentPattern.MatchString(result.TaskID) {
		return "", fmt.Errorf("create returned no valid TaskId; inspect provider before recovery")
	}
	receipt.TaskID = result.TaskID
	// Log the ID before persisting, so an operator can recover ambiguous writes.
	slog.Info("created CPFS import", "taskId", result.TaskID, "generation", cfg.Generation)
	tmp := name + "-" + randomID()
	f, err = parent.OpenFile(tmp, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
	if err != nil {
		return "", err
	}
	defer parent.Remove(tmp)
	err = json.NewEncoder(f).Encode(receipt)
	if err == nil {
		err = f.Sync()
	}
	closeErr = f.Close()
	if err == nil {
		err = closeErr
	}
	if err != nil {
		return "", err
	}
	if err := parent.Rename(tmp, name); err != nil {
		return "", err
	}
	return result.TaskID, nil
}

type cloudTask struct {
	TaskID, FilesystemID, DataFlowID, Status, TaskAction, DataType, Directory, DstDirectory, ErrorMsg string
	ProgressStats                                                                                     *struct{ FilesTotal, FilesDone, BytesTotal, BytesDone *int64 }
	Reports                                                                                           struct{ Report []struct{ Name, Path string } }
}

func (c *dataFlowClient) describe(ctx context.Context, cfg Config, taskID string) (*cloudTask, error) {
	args := url.Values{"FileSystemId": {cfg.DataFlow.FileSystemID}, "Filters.1.Key": {"TaskIds"}, "Filters.1.Value": {taskID}, "Filters.2.Key": {"DataFlowIds"}, "Filters.2.Value": {cfg.DataFlow.DataFlowID}, "MaxResults": {"100"}, "WithReports": {"true"}}
	var result struct {
		NextToken string
		TaskInfo  struct{ Task []cloudTask }
	}
	if err := c.call(ctx, "DescribeDataFlowTasks", args, &result); err != nil {
		return nil, err
	}
	var found *cloudTask
	for i := range result.TaskInfo.Task {
		task := &result.TaskInfo.Task[i]
		if task.TaskID == taskID {
			if found != nil {
				return nil, fmt.Errorf("duplicate cloud task identity")
			}
			found = task
		}
	}
	if found == nil {
		return nil, fmt.Errorf("cloud task %s not found; retaining data", taskID)
	}
	dest, err := dataFlowDestination(cfg)
	if err != nil {
		return nil, err
	}
	if found.FilesystemID != cfg.DataFlow.FileSystemID || found.DataFlowID != cfg.DataFlow.DataFlowID || found.TaskAction != "Import" || found.DataType != "MetaAndData" || found.Directory != "/"+cfg.Source+"/" || found.DstDirectory != dest {
		return nil, fmt.Errorf("cloud task %s identity/paths differ from requested import", taskID)
	}
	return found, nil
}

func stageDataFlow(ctx context.Context, cfg Config, parent, dst *os.Root, c *dataFlowClient) error {
	taskID, err := c.task(ctx, cfg, parent)
	if err != nil {
		return err
	}
	for {
		task, err := c.describe(ctx, cfg, taskID)
		if err != nil {
			return err
		}
		switch task.Status {
		case "Pending", "Executing":
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(c.poll):
			}
		case "Completed":
			if task.ErrorMsg != "" {
				return fmt.Errorf("cloud task %s reports an error despite completion", taskID)
			}
			for _, report := range task.Reports.Report {
				if (report.Name == "FailedFilesReport" || report.Name == "SkippedFilesReport") && report.Path != "" {
					return fmt.Errorf("cloud task %s has a failed/skipped report requiring inspection", taskID)
				}
			}
			p := task.ProgressStats
			if p == nil || p.FilesTotal == nil || p.FilesDone == nil || p.BytesTotal == nil || p.BytesDone == nil || *p.FilesTotal < 0 || *p.FilesTotal != *p.FilesDone || *p.BytesTotal != cfg.ExpectedBytes || *p.BytesDone != cfg.ExpectedBytes {
				return fmt.Errorf("cloud task %s progress does not confirm all expected data", taskID)
			}
			if err := verifyOwner(dst, cfg); err != nil {
				return err
			}
			_, total, err := snapshot(ctx, dst, true)
			if err != nil {
				return err
			}
			if total != cfg.ExpectedBytes {
				return fmt.Errorf("imported byte count %d does not equal expectedBytes %d", total, cfg.ExpectedBytes)
			}
			return nil
		case "Failed", "Canceled", "Canceling":
			return fmt.Errorf("cloud task %s is %s; data retained", taskID, task.Status)
		default:
			return fmt.Errorf("cloud task %s has unknown state %q; data retained", taskID, task.Status)
		}
	}
}
