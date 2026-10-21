package config

import (
	"os"
	"strconv"
)

func loadString(env string, def string) string {
	if cur := os.Getenv(env); cur != "" {
		return cur
	}
	return def
}

func loadInt(env string, def int) int {
	cur := os.Getenv(env)
	if cur != "" {
		if val, err := strconv.Atoi(cur); err == nil && val > 0 {
			return val
		}
	}
	return def
}
