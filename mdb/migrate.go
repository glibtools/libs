package mdb

import (
	"os"
	"path/filepath"

	"github.com/glibtools/libs/util"
)

const migrateLock = "migrate.lock"

func createMigrateLockFile() {
	_ = os.WriteFile(migrateLockFilePath(), []byte("1"), 0644)
}

func fileExists(file string) bool {
	_, err := os.Stat(file)
	return err == nil || os.IsExist(err)
}

func hasMigrateLockFile() bool { return fileExists(migrateLockFilePath()) }

func migrateLockFilePath() string { return filepath.Join(util.RootDir(), "data", migrateLock) }
