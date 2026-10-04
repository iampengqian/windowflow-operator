package worker

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"math"
	"os"
	"path"
	"path/filepath"
	"strings"
	"syscall"
	"time"
)

const ownerName = ".windowflow-owner.json"
const maxMarkerBytes = 4096

type owner struct {
	Version    int    `json:"version"`
	PlanUID    string `json:"planUID"`
	Generation string `json:"generation"`
	Digest     string `json:"configDigest"`
}

type treeEntry struct {
	name string
	info fs.FileInfo
}

// Run executes one worker action. A stage failure leaves the owned directory
// intact for diagnosis; only an explicitly authorized clean action removes it.
func Run(ctx context.Context, cfg Config, cacheRoot, sourceRoot string) error {
	if err := Validate(cfg); err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	cache, err := openRoot(cacheRoot)
	if err != nil {
		return fmt.Errorf("open cache: %w", err)
	}
	defer cache.Close()
	parentPath := path.Dir(cfg.RelativePath)
	if err := ensureDirectories(cache, parentPath, cfg.Action == "stage"); err != nil {
		if cfg.Action == "clean" && errors.Is(err, fs.ErrNotExist) {
			return nil
		}
		return err
	}
	parent, err := cache.OpenRoot(parentPath)
	if err != nil {
		return err
	}
	defer parent.Close()
	unlock, err := lockGeneration(ctx, parent, cfg.Generation)
	if err != nil {
		return err
	}
	defer unlock()
	if cfg.Action == "clean" {
		return clean(ctx, parent, cfg)
	}
	if cfg.Backend == "local" {
		if err := distinctRoots(cacheRoot, sourceRoot); err != nil {
			return err
		}
		source, err := openRoot(sourceRoot)
		if err != nil {
			return fmt.Errorf("open source: %w", err)
		}
		defer source.Close()
		if err := ensureDirectories(source, cfg.Source, false); err != nil {
			return fmt.Errorf("source: %w", err)
		}
		window, err := source.OpenRoot(cfg.Source)
		if err != nil {
			return err
		}
		defer window.Close()
		return stageLocal(ctx, parent, window, cfg)
	}
	dst, err := ownedDirectory(parent, cfg, true)
	if err != nil {
		return err
	}
	defer dst.Close()
	if _, _, err := snapshot(ctx, dst, true); err != nil {
		return err
	}
	client, err := newDataFlowClient(cfg.DataFlow)
	if err != nil {
		return err
	}
	return stageDataFlow(ctx, cfg, parent, dst, client)
}

func openRoot(name string) (*os.Root, error) {
	info, err := os.Lstat(name)
	if err != nil {
		return nil, err
	}
	if !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
		return nil, fmt.Errorf("root must be a real directory")
	}
	r, err := os.OpenRoot(name)
	if err != nil {
		return nil, err
	}
	after, err := r.Stat(".")
	if err != nil || !os.SameFile(info, after) {
		r.Close()
		return nil, fmt.Errorf("root changed while opening")
	}
	return r, nil
}

func distinctRoots(cache, source string) error {
	c, err := filepath.EvalSymlinks(cache)
	if err != nil {
		return err
	}
	s, err := filepath.EvalSymlinks(source)
	if err != nil {
		return err
	}
	c, err = filepath.Abs(c)
	if err != nil {
		return err
	}
	s, err = filepath.Abs(s)
	if err != nil {
		return err
	}
	if c == s || strings.HasPrefix(c, s+string(os.PathSeparator)) || strings.HasPrefix(s, c+string(os.PathSeparator)) {
		return fmt.Errorf("source and cache roots must be separate, non-nested directories")
	}
	ci, err := os.Stat(c)
	if err != nil {
		return err
	}
	si, err := os.Stat(s)
	if err != nil {
		return err
	}
	if os.SameFile(ci, si) {
		return fmt.Errorf("source and cache refer to the same directory")
	}
	return nil
}

func ensureDirectories(root *os.Root, name string, create bool) error {
	prefix := ""
	for _, component := range strings.Split(name, "/") {
		prefix = path.Join(prefix, component)
		info, err := root.Lstat(prefix)
		if errors.Is(err, fs.ErrNotExist) && create {
			if err = root.Mkdir(prefix, 0755); err != nil && !errors.Is(err, fs.ErrExist) {
				return err
			}
			info, err = root.Lstat(prefix)
		}
		if err != nil {
			return err
		}
		if !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
			return fmt.Errorf("unsafe directory component %q", prefix)
		}
	}
	return nil
}

// Locks live beside generation directories and survive cleanup. Advisory flock
// serializes duplicate Jobs, and the OS releases it if a worker crashes. This
// reference worker targets Linux (ACK/container) and macOS (development).
func lockGeneration(ctx context.Context, parent *os.Root, generation string) (func(), error) {
	name := "." + generation + ".lock"
	if info, err := parent.Lstat(name); err == nil && !info.Mode().IsRegular() {
		return nil, fmt.Errorf("unsafe worker lock")
	} else if err != nil && !errors.Is(err, fs.ErrNotExist) {
		return nil, err
	}
	f, err := parent.OpenFile(name, os.O_CREATE|os.O_RDWR|syscall.O_NOFOLLOW, 0600)
	if err != nil {
		return nil, err
	}
	info, err := f.Stat()
	if err != nil || !info.Mode().IsRegular() {
		f.Close()
		return nil, fmt.Errorf("worker lock is not regular")
	}
	for {
		err = syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB)
		if err == nil {
			return func() { _ = syscall.Flock(int(f.Fd()), syscall.LOCK_UN); _ = f.Close() }, nil
		}
		if !errors.Is(err, syscall.EWOULDBLOCK) && !errors.Is(err, syscall.EAGAIN) {
			f.Close()
			return nil, fmt.Errorf("lock generation: %w", err)
		}
		select {
		case <-ctx.Done():
			f.Close()
			return nil, ctx.Err()
		case <-time.After(100 * time.Millisecond):
		}
	}
}

func expectedOwner(cfg Config) owner { return owner{1, cfg.PlanUID, cfg.Generation, configDigest(cfg)} }

func ownedDirectory(parent *os.Root, cfg Config, create bool) (*os.Root, error) {
	created := false
	info, err := parent.Lstat(cfg.Generation)
	if errors.Is(err, fs.ErrNotExist) && create {
		err = parent.Mkdir(cfg.Generation, 0755)
		if err != nil {
			return nil, err
		}
		created = true
		info, err = parent.Lstat(cfg.Generation)
	}
	if err != nil {
		return nil, err
	}
	if !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
		return nil, fmt.Errorf("generation is not a real directory")
	}
	r, err := parent.OpenRoot(cfg.Generation)
	if err != nil {
		return nil, err
	}
	if actual, err := r.Stat("."); err != nil || !os.SameFile(info, actual) {
		r.Close()
		return nil, fmt.Errorf("generation directory changed")
	}
	if created {
		data, _ := json.Marshal(expectedOwner(cfg))
		f, err := r.OpenFile(ownerName, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
		if err != nil {
			r.Close()
			return nil, err
		}
		_, err = f.Write(data)
		if err == nil {
			err = f.Sync()
		}
		closeErr := f.Close()
		if err == nil {
			err = closeErr
		}
		if err != nil {
			r.Close()
			return nil, err
		}
	}
	if err := verifyOwner(r, cfg); err != nil {
		r.Close()
		return nil, err
	}
	return r, nil
}

func verifyOwner(root *os.Root, cfg Config) error {
	info, err := root.Lstat(ownerName)
	if err != nil {
		return fmt.Errorf("ownership marker missing: %w", err)
	}
	if !info.Mode().IsRegular() || info.Size() > maxMarkerBytes {
		return fmt.Errorf("invalid ownership marker")
	}
	f, err := root.OpenFile(ownerName, os.O_RDONLY|syscall.O_NOFOLLOW, 0)
	if err != nil {
		return err
	}
	defer f.Close()
	var actual owner
	decoder := json.NewDecoder(io.LimitReader(f, maxMarkerBytes+1))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&actual); err != nil {
		return fmt.Errorf("invalid ownership marker: %w", err)
	}
	var extra any
	if err := decoder.Decode(&extra); err != io.EOF {
		return fmt.Errorf("invalid ownership marker trailing content")
	}
	if actual != expectedOwner(cfg) {
		return fmt.Errorf("ownership marker does not match this plan, generation and source")
	}
	return nil
}

func snapshot(ctx context.Context, root *os.Root, skipOwner bool) ([]treeEntry, int64, error) {
	var entries []treeEntry
	var bytes int64
	err := fs.WalkDir(root.FS(), ".", func(name string, d fs.DirEntry, walkErr error) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		if walkErr != nil {
			return walkErr
		}
		info, err := root.Lstat(name)
		if err != nil {
			return err
		}
		if info.Mode()&os.ModeSymlink != 0 || (!info.IsDir() && !info.Mode().IsRegular()) {
			return fmt.Errorf("symlink or special file rejected: %s", name)
		}
		if name == ownerName && skipOwner {
			return nil
		}
		if !skipOwner && name != "." && strings.HasPrefix(path.Base(name), ".windowflow-") {
			return fmt.Errorf("reserved file name: %s", name)
		}
		if info.Mode().IsRegular() {
			if info.Size() < 0 || bytes > math.MaxInt64-info.Size() {
				return fmt.Errorf("file size sum overflow")
			}
			bytes += info.Size()
		}
		entries = append(entries, treeEntry{name, info})
		return nil
	})
	return entries, bytes, err
}

func sameEntry(a, b fs.FileInfo) bool {
	return os.SameFile(a, b) && a.Mode() == b.Mode() && a.Size() == b.Size() && a.ModTime().Equal(b.ModTime())
}

func stageLocal(ctx context.Context, parent, source *os.Root, cfg Config) error {
	before, total, err := snapshot(ctx, source, false)
	if err != nil {
		return err
	}
	if total != cfg.ExpectedBytes {
		return fmt.Errorf("source byte count %d does not equal expectedBytes %d", total, cfg.ExpectedBytes)
	}
	dst, err := ownedDirectory(parent, cfg, true)
	if err != nil {
		return err
	}
	defer dst.Close()
	existing, _, err := snapshot(ctx, dst, true)
	if err != nil {
		return err
	}
	wanted := make(map[string]fs.FileMode, len(before))
	for _, e := range before {
		wanted[e.name] = e.info.Mode().Type()
	}
	for _, e := range existing {
		mode, ok := wanted[e.name]
		if !ok || mode != e.info.Mode().Type() {
			return fmt.Errorf("unexpected existing destination entry %q; refusing to remove it", e.name)
		}
	}
	for _, e := range before {
		if e.name == "." {
			continue
		}
		if e.info.IsDir() {
			if err := ensureDirectories(dst, e.name, true); err != nil {
				return err
			}
			continue
		}
		if err := copyFile(ctx, source, dst, e); err != nil {
			return err
		}
	}
	after, afterBytes, err := snapshot(ctx, source, false)
	if err != nil {
		return err
	}
	if afterBytes != total || len(before) != len(after) {
		return fmt.Errorf("source tree changed during copy")
	}
	for i := range before {
		if before[i].name != after[i].name || !sameEntry(before[i].info, after[i].info) {
			return fmt.Errorf("source changed during copy: %s", before[i].name)
		}
	}
	if err := verifyOwner(dst, cfg); err != nil {
		return err
	}
	_, copied, err := snapshot(ctx, dst, true)
	if err != nil {
		return err
	}
	if copied != cfg.ExpectedBytes {
		return fmt.Errorf("destination byte count %d does not equal expectedBytes %d", copied, cfg.ExpectedBytes)
	}
	return nil
}

func copyFile(ctx context.Context, source, dst *os.Root, e treeEntry) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	pre, err := source.Lstat(e.name)
	if err != nil || !sameEntry(e.info, pre) {
		return fmt.Errorf("source changed before copy: %s", e.name)
	}
	in, err := source.OpenFile(e.name, os.O_RDONLY|syscall.O_NOFOLLOW, 0)
	if err != nil {
		return err
	}
	defer in.Close()
	opened, err := in.Stat()
	if err != nil || !sameEntry(e.info, opened) {
		return fmt.Errorf("source changed while opening: %s", e.name)
	}
	// Exclusive temporary regular files plus file rename avoid following an
	// existing destination symlink or writing through a hardlink on retries.
	tmp := path.Join(path.Dir(e.name), ".windowflow-copy-"+randomID())
	out, err := dst.OpenFile(tmp, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0644)
	if err != nil {
		return err
	}
	defer func() { _ = out.Close(); _ = dst.Remove(tmp) }()
	n, err := io.Copy(out, &contextReader{ctx, io.LimitReader(in, e.info.Size()+1)})
	if err != nil {
		return err
	}
	if n != e.info.Size() {
		return fmt.Errorf("source size changed while copying %s", e.name)
	}
	if err := out.Sync(); err != nil {
		return err
	}
	if err := out.Close(); err != nil {
		return err
	}
	post, err := in.Stat()
	if err != nil || !sameEntry(e.info, post) {
		return fmt.Errorf("source changed while copying %s", e.name)
	}
	if err := dst.Rename(tmp, e.name); err != nil {
		return err
	}
	return nil
}

type contextReader struct {
	ctx    context.Context
	reader io.Reader
}

func (r *contextReader) Read(p []byte) (int, error) {
	if err := r.ctx.Err(); err != nil {
		return 0, err
	}
	return r.reader.Read(p)
}

func clean(ctx context.Context, parent *os.Root, cfg Config) error {
	if _, err := parent.Lstat(cfg.Generation); errors.Is(err, fs.ErrNotExist) {
		return nil
	} else if err != nil {
		return err
	}
	dst, err := ownedDirectory(parent, cfg, false)
	if err != nil {
		return err
	}
	defer dst.Close()
	entries, _, err := snapshot(ctx, dst, true)
	if err != nil {
		return err
	}
	// Delete children through the opened generation root. Never use a recursive
	// delete rooted at cacheRoot or a provider API that could affect the OSS source.
	for i := len(entries) - 1; i >= 0; i-- {
		if entries[i].name == "." {
			continue
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		actual, err := dst.Lstat(entries[i].name)
		if err != nil {
			return err
		}
		if !os.SameFile(actual, entries[i].info) || actual.Mode().Type() != entries[i].info.Mode().Type() {
			return fmt.Errorf("destination changed during clean")
		}
		if err := dst.Remove(entries[i].name); err != nil {
			return err
		}
	}
	if err := verifyOwner(dst, cfg); err != nil {
		return err
	}
	opened, err := dst.Stat(".")
	if err != nil {
		return err
	}
	current, err := parent.Lstat(cfg.Generation)
	if err != nil {
		return err
	}
	if !os.SameFile(opened, current) {
		return fmt.Errorf("generation path changed during clean")
	}
	if err := dst.Remove(ownerName); err != nil {
		return err
	}
	if err := parent.Remove(cfg.Generation); err != nil {
		// Restore the marker if a late unexpected entry prevented final removal.
		data, _ := json.Marshal(expectedOwner(cfg))
		_ = dst.WriteFile(ownerName, data, 0600)
		return err
	}
	return nil
}
