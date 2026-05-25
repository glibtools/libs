package config

import (
	"log"
	"os"
	"path/filepath"
	"sync"

	"github.com/fsnotify/fsnotify"
	"github.com/spf13/viper"

	"github.com/glibtools/libs/util"
)

var C = new(Config)
var globalDefaults = map[string]interface{}{
	"app.name":      util.AppName,
	"app.host":      "127.0.0.1",
	"app.debug":     true,
	"app.runtimes":  0,
	"app.log_level": "debug",

	"server.host":             "80",
	"server.pprof_port":       "81",
	"server.concurrent_limit": 1000,
	"server.rate_limit_value": 20,
	"server.rate_limit_burst": 50,
	"server.debug":            true,
	"server.crypto":           "",

	"db.type":                  "",
	"db.host":                  "",
	"db.db":                    "",
	"db.user":                  "",
	"db.port":                  "",
	"db.pwd":                   "",
	"db.debug":                 true,
	"db.log_level":             4,
	"db.skip_cache":            false,
	"db.skip_create_db":        false,
	"db.cache_type":            "mem",
	"db.max_idle_conns":        10,
	"db.max_open_conns":        200,
	"db.max_lifetime_seconds":  60,
	"db.max_idle_time_seconds": 30,

	"redis.host": "",
	"redis.port": "",
	"redis.db":   0,
	"redis.pwd":  "",

	"emq.host":           "",
	"emq.port":           "",
	"emq.super_username": "",
	"emq.super_password": "",
}

type Config struct {
	v *viper.Viper

	configFile string

	mutex sync.RWMutex

	BeforeLoad func(v *viper.Viper)
}

// AppDebug return bool value
func (c *Config) AppDebug() bool {
	return c.v.GetBool("app.debug")
}

// AppName ......
func (c *Config) AppName() string {
	appName := c.v.GetString("app.name")
	if appName == "" {
		appName = util.AppName
	}
	return appName
}

func (c *Config) DBDebug() bool {
	return c.v.GetBool("db.debug")
}

// GetConfigFile ......
func (c *Config) GetConfigFile() string {
	if c.configFile == "" {
		c.configFile = filepath.Join(util.RootDir(), "conf.toml")
	}
	return c.configFile
}

func (c *Config) Load() { c.constructor() }

// SetConfigFile ......
func (c *Config) SetConfigFile(f string) { c.configFile = f }

// SetValue ...
func (c *Config) SetValue(k string, val interface{}) {
	c.Write2tempCall(func(v *viper.Viper) {
		v.Set(k, val)
	})
}

func (c *Config) SetViper(v *viper.Viper) { c.v = v }

func (c *Config) V() *viper.Viper { return c.v }

func (c *Config) Write2tempCall(fn func(v *viper.Viper)) {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	v := viper.New()
	e := v.MergeConfigMap(c.v.AllSettings())
	if e != nil {
		log.Printf("merge config map error: %s\n", e.Error())
	}
	if fn != nil {
		fn(v)
	}
	e = v.WriteConfigAs(tempConfigFile(c.GetConfigFile()))
	if e != nil {
		log.Printf("write config file error: %s\n", e.Error())
	}
}

// constructor ......
func (c *Config) constructor() *Config {
	if e := c.load(); e != nil {
		log.Fatalf("load config file error: %s", e.Error())
	}
	return c
}

// load ......
func (c *Config) load() (err error) {
	c.v = viper.New()
	func(v *viper.Viper) {
		f := c.GetConfigFile()
		log.Printf("load config file: %s\n", f)
		swapTempConfigFile(f)
		v.SetConfigFile(f)
		for k, v1 := range globalDefaults {
			v.SetDefault(k, v1)
		}

		if c.BeforeLoad != nil {
			c.BeforeLoad(v)
		}

		if err = c.writeDefaultConfigIfNotExists(); err != nil {
			return
		}

		if err = v.ReadInConfig(); err != nil {
			return
		}

		v.OnConfigChange(func(event fsnotify.Event) {
			//log.Printf("config file changed: %s", event.Name)
			//spew.Dump(v.Sub("app").AllSettings())
			c.Write2tempCall(nil)
		})
		v.WatchConfig()
	}(c.v)
	return
}

// writeDefaultConfigIfNotExists ......
func (c *Config) writeDefaultConfigIfNotExists() error {
	f := c.GetConfigFile()
	_, e := os.Stat(f)
	if os.IsNotExist(e) {
		_ = os.MkdirAll(filepath.Dir(f), os.ModePerm)
		return c.v.WriteConfig()
	}
	return e
}

func MergeGlobalDefaults(m map[string]interface{}) {
	for k, v := range m {
		globalDefaults[k] = v
	}
}

func Viper() *viper.Viper { return C.v }

func swapTempConfigFile(name string) {
	if !tempExists(name) {
		return
	}
	// rename temp config file
	e := os.Rename(tempConfigFile(name), name)
	if e != nil {
		log.Printf("rename config file error: %s\n", e.Error())
	}
}

func tempConfigFile(name string) string {
	return name + "_temp.toml"
}

func tempExists(name string) bool {
	_, e := os.Stat(tempConfigFile(name))
	return !os.IsNotExist(e)
}
