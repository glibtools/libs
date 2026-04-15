package config

import (
	"log"
	"path/filepath"

	"github.com/glibtools/libs/env"
	"github.com/glibtools/libs/util"
)

func LoadConfig() {
	env.LoadEnvironment(filepath.Join(util.RootDir(), "pro.env"))
	configFile := env.GetEnv("CONFIG_FILE", "")
	func() {
		if configFile != "" {
			C.SetConfigFile(configFile)
			return
		}
		log.Printf("env: CONFIG_FILE is not set; using default config file")
	}()
	C.Load()
}
