package util

import (
	"os"
	"path/filepath"
)

var RootDir = func() string {
	ec, _ := os.Executable()
	return filepath.Dir(ec)
}
