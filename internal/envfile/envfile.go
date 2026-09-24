package envfile

import (
	"errors"
	"os"
	"path/filepath"

	"github.com/joho/godotenv"
)

// findModuleRoot looks up to find .env file
func findModuleRoot(start string) (string, error) {
	dir, err := filepath.Abs(start)
	if err != nil {
		return "", err
	}
	for {
		if _, err := os.Stat(filepath.Join(dir, ".env")); err == nil {
			return dir, nil
		}
		parent := filepath.Dir(dir)
		if parent == dir { // достигли корня файловой системы
			return "", errors.New(".env not found")
		}
		dir = parent
	}
}

func LoadEnv() error {
	wd, err := os.Getwd()
	if err != nil {
		return err
	}
	root, err := findModuleRoot(wd)
	if err != nil {
		return nil
	}
	return godotenv.Load(filepath.Join(root, ".env"))
}
