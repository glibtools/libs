package mdb

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"sync"
	"sync/atomic"

	"github.com/spf13/cast"
	"gorm.io/gorm"
	"gorm.io/gorm/schema"

	"github.com/glibtools/libs/mdb/gormcache"
)

// NoCache disables this plugin for the current GORM statement.
// Value forwards to gormcache.NoCache so callers using
// db.Set(mdb.NoCache, true) keep working against the underlying plugin.
const NoCache = gormcache.NoCache

// Gm2cCacheStat exposes hit/miss counters as a package var so callers may
// read them via atomic.LoadUint64(&mdb.Gm2cCacheStat.Hit).
var Gm2cCacheStat = Gm2cStats{}

// pluginRegistry maps Gm2cConfig.Prefix -> *gormcache.Plugin so that
// (*Gm2cConfig).ClearBean / ClearBeanByID / ClearTable can locate the
// underlying plugin instance after NewPlugin has been called.
var pluginRegistry sync.Map

// CacheStore is the cache backend accepted by Gm2cConfig.
// It is intentionally an independent interface (not a type alias) so the
// mdb package keeps a stable surface. Because its method set matches
// gormcache.Store structurally, a CacheStore value is assignable to a
// gormcache.Store parameter without conversion.
type CacheStore interface {
	ItfStorageCache
}

// Gm2cConfig configures the single-record cache plugin. Field names, order,
// and exported types remain stable for external callers.
type Gm2cConfig struct {
	Skip   bool
	Prefix string
	TTL    int64 // seconds
	Store  CacheStore
}

// ClearBean invalidates the cache entry for the single row identified by bean.
func (g *Gm2cConfig) ClearBean(bean interface{}) error {
	if g == nil || g.Store == nil {
		return nil
	}
	table, pk, err := cacheIdentityFromBean(bean)
	if err != nil {
		return err
	}
	if p := lookupPlugin(g.Prefix); p != nil {
		p.InvalidateRow(table, pk)
	}
	return nil
}

// ClearBeanByID invalidates the cache entry for table(model) row with primary key id.
func (g *Gm2cConfig) ClearBeanByID(model, id interface{}) error {
	if g == nil || g.Store == nil {
		return nil
	}
	table, err := cacheTableFromModel(model)
	if err != nil {
		return err
	}
	pk, err := cachePrimaryKeyString(id)
	if err != nil {
		return err
	}
	if p := lookupPlugin(g.Prefix); p != nil {
		p.InvalidateRow(table, pk)
	}
	return nil
}

// ClearTable invalidates every cached row for table.
func (g *Gm2cConfig) ClearTable(table string) {
	if g == nil || g.Store == nil {
		return
	}
	if p := lookupPlugin(g.Prefix); p != nil {
		p.InvalidateTable(table)
	}
}

// Gm2cPlugin adapts a gormcache.Plugin to the legacy mdb plugin shape.
type Gm2cPlugin struct {
	cfg    Gm2cConfig
	plugin *gormcache.Plugin
}

// Initialize installs the underlying gormcache.Plugin and registers a
// post-query callback that mirrors gormcache counters into Gm2cCacheStat.
func (p *Gm2cPlugin) Initialize(db *gorm.DB) error {
	if err := p.plugin.Initialize(db); err != nil {
		return err
	}
	return db.Callback().Query().After("gorm:query").Register("gm2c:stats:sync", p.syncStats)
}

// Name preserves the legacy plugin name for backwards compatibility with any
// caller that inspects db.Plugins["single-record-cache"].
func (p *Gm2cPlugin) Name() string { return "single-record-cache" }

func (p *Gm2cPlugin) syncStats(*gorm.DB) {
	if p.plugin == nil {
		return
	}
	stats := p.plugin.Stats()
	atomic.StoreUint64(&Gm2cCacheStat.Hit, stats.Hits)
	atomic.StoreUint64(&Gm2cCacheStat.Miss, stats.Misses)
}

// Gm2cStats holds atomic counters published by the cache plugin.
type Gm2cStats struct {
	Hit  uint64 `json:"hit"`
	Miss uint64 `json:"miss"`
}

// NewPlugin builds a gorm.Plugin that wraps a gormcache.Plugin configured
// from cfg. The returned plugin is registered under cfg.Prefix so that the
// caller's *Gm2cConfig can later locate it for invalidations.
func NewPlugin(cfg Gm2cConfig) gorm.Plugin {
	gcCfg := gormcache.Config{
		Prefix:             cfg.Prefix,
		Disabled:           cfg.Skip,
		Store:              cfg.Store,
		TTLSeconds:         cfg.TTL,
		NegativeTTLSeconds: 60,
	}
	inner := gormcache.New(gcCfg)
	if cfg.Prefix != "" {
		pluginRegistry.Store(cfg.Prefix, inner)
	}
	return &Gm2cPlugin{cfg: cfg, plugin: inner}
}

func cacheIdentityFromBean(bean interface{}) (table, pk string, err error) {
	if bean == nil {
		return "", "", errors.New("bean is nil")
	}
	s, rv, err := parseCacheModel(bean)
	if err != nil {
		return "", "", err
	}
	if len(s.PrimaryFields) != 1 {
		return "", "", fmt.Errorf("bean %T must have exactly one primary key", bean)
	}
	v, zero := s.PrimaryFields[0].ValueOf(context.Background(), rv)
	if zero {
		return "", "", fmt.Errorf("bean %T primary key is zero", bean)
	}
	return s.Table, cast.ToString(v), nil
}

func cachePrimaryKeyString(id interface{}) (string, error) {
	if id == nil {
		return "", errors.New("id is nil")
	}
	rv := reflect.ValueOf(id)
	for rv.Kind() == reflect.Pointer {
		if rv.IsNil() {
			return "", errors.New("id is nil")
		}
		rv = rv.Elem()
	}
	if !rv.IsValid() || rv.IsZero() {
		return "", errors.New("id is zero")
	}
	return cast.ToString(rv.Interface()), nil
}

func cacheTableFromModel(model interface{}) (string, error) {
	s, _, err := parseCacheModel(model)
	if err != nil {
		return "", err
	}
	if len(s.PrimaryFields) != 1 {
		return "", fmt.Errorf("model %T must have exactly one primary key", model)
	}
	return s.Table, nil
}

func lookupPlugin(prefix string) *gormcache.Plugin {
	if prefix == "" {
		return nil
	}
	v, ok := pluginRegistry.Load(prefix)
	if !ok {
		return nil
	}
	p, _ := v.(*gormcache.Plugin)
	return p
}

func parseCacheModel(model interface{}) (s *schema.Schema, rv reflect.Value, err error) {
	if model == nil {
		return nil, reflect.Value{}, errors.New("model is nil")
	}
	s, err = ParseModel(model)
	if err != nil {
		return nil, reflect.Value{}, err
	}
	rv = reflect.ValueOf(model)
	for rv.Kind() == reflect.Pointer {
		if rv.IsNil() {
			return nil, reflect.Value{}, errors.New("model is nil")
		}
		rv = rv.Elem()
	}
	if rv.Kind() != reflect.Struct {
		return nil, reflect.Value{}, fmt.Errorf("model %T must be a struct or struct pointer", model)
	}
	return s, rv, nil
}
