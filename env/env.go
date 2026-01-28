package env

import (
	"os"
	"path/filepath"

	"github.com/joho/godotenv"

	"github.com/glibtools/libs/util"
)

const envFile = ".env"

func GetEnv(key, fallback string) string {
	// get environment variables
	value, ok := os.LookupEnv(key)
	if !ok {
		value = fallback
	}
	return value
}

func LoadEnvironment(files ...string) {
	rootDir := util.RootDir()
	paths := []string{
		".",
		"../",
		rootDir,
		filepath.Dir(rootDir),
		filepath.Join(rootDir, "etc"),
		filepath.Join(rootDir, "data"),
	}
	for _, path := range paths {
		_ef := filepath.Join(path, envFile)
		if _, err := os.Stat(_ef); err == nil {
			_ = godotenv.Load(_ef)
		}
	}
	for _, file := range files {
		_ = godotenv.Load(file)
	}
}
