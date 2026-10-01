// internal/blob/localfs.go
package blob

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
)

type LocalFS struct{ root string }

func NewLocalFS(root string) *LocalFS { return &LocalFS{root: root} }

func (l *LocalFS) Put(ctx context.Context, key string, body io.Reader) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	path := filepath.Join(l.root, filepath.FromSlash(key))

	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return fmt.Errorf("mkdir: %w", err)
	}

	tmp, err := os.CreateTemp(filepath.Dir(path), ".tmp-*")
	if err != nil {
		return fmt.Errorf("create temp: %w", err)
	}
	defer os.Remove(tmp.Name()) // если переименование не дошло

	if _, err := io.Copy(tmp, body); err != nil {
		tmp.Close()
		return fmt.Errorf("write: %w", err)
	}
	if err := tmp.Close(); err != nil {
		return fmt.Errorf("close: %w", err)
	}
	return os.Rename(tmp.Name(), path)
}
