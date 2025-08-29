package mdb

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/fatih/color"
	"github.com/spf13/cast"
	"golang.org/x/sync/singleflight"
	"gorm.io/gorm"
	"gorm.io/gorm/callbacks"
	"gorm.io/gorm/clause"
)

const (
	NoCache = "no-cache"

	globalPrefix = "gm2c"
	separator    = ":sep:"
	nullValue    = "null"

	gcInterval = time.Second * 60 * 10
)

var (
	PCacheStat        = new(PluginCacheStat)
	_                 = color.Red
	localSingleFlight = singleflight.Group{}
	// reuse null byte slice to avoid repeated allocations when comparing/storing null marker
	nullBytes = []byte(nullValue)
	// cache compiled regexps per primary key name to avoid recompiling each call
	reRegexCache sync.Map // map[string]*regexp.Regexp
)

type CacheStore interface {
	Get(key string) (data []byte, ok bool)
	Set(key string, data []byte, ttl ...int64)
	Del(key string)
	ClearAll()
	DropPrefix(prefix ...string)
}

type Gm2cConfig struct {
	Skip bool
	// TTL is the time to live in seconds, default is 0, means no expiration
	TTL    int64
	Prefix string
	Store  CacheStore
}

func (c *Gm2cConfig) ClearAll() {
	//color.Red("clear all")
	c.Store.ClearAll()
}

func (c *Gm2cConfig) ClearTable(table string) {
	//color.Red("clear table: %s", table)
	c.DropPrefix(tablePrefix(c.Prefix, table))
}

func (c *Gm2cConfig) Del(key string) {
	//color.Red("del key: %s", key)
	c.Store.Del(key)
}

func (c *Gm2cConfig) DropPrefix(prefix ...string) {
	/*for _, _pre := range prefix {
		color.Red("drop prefix: %s", _pre)
	}*/
	c.Store.DropPrefix(prefix...)
}

func (c *Gm2cConfig) Get(key string) (data []byte, ok bool) {
	//color.Green("get key: %s", key)
	return c.Store.Get(key)
}

func (c *Gm2cConfig) Set(key string, data []byte, ttl ...int64) {
	//color.Cyan("set key: %s", key)
	c.Store.Set(key, data, ttl...)
}

type PluginCacheStat struct {
	SearchHit   uint64
	SearchMiss  uint64
	PrimaryHit  uint64
	PrimaryMiss uint64
}

type StoreGC interface {
	StoreGC(prefix string)
}

type gm2call struct {
	Data json.RawMessage

	RowsAffected int64
}

type gormPluginCache struct {
	Gm2cConfig
}

func (p *gormPluginCache) Initialize(db *gorm.DB) (err error) {
	if p.Skip {
		return
	}

	_ = db.Callback().Query().Replace("gorm:query", p.query)

	createCallback := db.Callback().Create()
	_ = createCallback.After("gorm:create").Register("plugin-cache:afterCreate", p.afterCreate)

	updateCallback := db.Callback().Update()
	_ = updateCallback.After("gorm:update").Register("plugin-cache:afterUpdate", p.afterUpdate)

	deleteCallback := db.Callback().Delete()
	_ = deleteCallback.After("gorm:delete").Register("plugin-cache:afterDelete", p.afterDelete)

	return
}

func (p *gormPluginCache) Name() string { return "plugin-cache" }

func (p *gormPluginCache) afterCreate(db *gorm.DB) {
	p.afterDelete(db)
}

func (p *gormPluginCache) afterDelete(db *gorm.DB) {
	_, noCache1 := db.Get(NoCache)
	_, noCache := db.InstanceGet(NoCache)
	if noCache1 || noCache {
		return
	}
	stm := db.Statement
	searchKey1 := searchKeyPrefix(p.Prefix, stm.Table)
	p.DropPrefix(searchKey1)
	//color.Red("afterDelete 1: key:%s", searchKey1)
	primaryKey1 := primaryKeyPrefix(p.Prefix, stm.Table)
	if stm.Schema == nil || stm.Schema.ModelType.Kind() != reflect.Struct {
		p.DropPrefix(primaryKey1)
		//color.Red("afterDelete 2: key:%s", primaryKey1)
		return
	}
	if stm.ReflectValue.Kind() != reflect.Struct {
		p.DropPrefix(primaryKey1)
		//color.Red("afterDelete 3: key:%s", primaryKey1)
		return
	}
	pk := p.findPrimaryCacheKey(stm, true)
	if pk != "" {
		p.Del(pk)
		//color.Red("afterDelete 4: key: %s", pk)
		return
	}
	_, idStr := getPrimaryCacheKVFromDest(stm)
	if idStr != "" {
		_key := primaryCacheKey(p.Prefix, stm.Table, idStr)
		p.Del(_key)
		//color.Red("afterDelete 5: key: %s", _key)
		return
	}
	_key := primaryKeyPrefix(p.Prefix, stm.Table)
	p.DropPrefix(_key)
	//color.Red("afterDelete 6: key:%s", _key)
}

func (p *gormPluginCache) afterUpdate(db *gorm.DB) {
	p.afterDelete(db)
}

func (p *gormPluginCache) findPrimaryCacheKey(stm *gorm.Statement, justHas bool) (val string) {
	primaryDBName := getPrimaryKeyName(stm)
	if primaryDBName == "" {
		return
	}

	cs, ok := stm.Clauses["WHERE"]
	if !ok {
		return
	}
	where, ok := cs.Expression.(clause.Where)
	if !ok {
		return
	}
	if len(where.Exprs) < 1 {
		return
	}

	m := make(map[string]struct{})
	var v string
	// must all expr be primary key?
	for _, expr := range where.Exprs {
		v = clauseFindPrimaryCacheKey(expr, primaryDBName)
		if v != "" {
			m[v] = struct{}{}
			if justHas {
				return primaryCacheKey(p.Prefix, stm.Table, v)
			}
		}
	}
	// must all conditions be primary key?
	if len(m) != 1 {
		return
	}
	val = primaryCacheKey(p.Prefix, stm.Table, v)
	return
}

func (p *gormPluginCache) getCache(stm *gorm.Statement) (hit bool) {
	key, isPrimary := p.getCacheKey(stm)
	//color.Yellow("start getCache: %s, isPrimary:%v", key, isPrimary)
	if key == "" {
		return
	}
	if !isPrimary {
		key, hit = p.queryCacheSearch(stm, key)
		if hit {
			atomic.AddUint64(&PCacheStat.SearchHit, 1)
		}
	}
	if key == "" {
		return
	}
	hit = p.queryCachePk(stm, key)
	if hit {
		atomic.AddUint64(&PCacheStat.PrimaryHit, 1)
	}
	return
}

func (p *gormPluginCache) getCacheKey(stm *gorm.Statement) (key string, isPrimary bool) {
	pk := p.findPrimaryCacheKey(stm, false)
	if pk != "" {
		return pk, true
	}
	sq := stm.DB.ToSQL(func(tx *gorm.DB) *gorm.DB { return tx })
	return searchCacheKey(p.Prefix, stm.Table, sq), false
}

func (p *gormPluginCache) query(db *gorm.DB) {
	if db.Error != nil {
		return
	}
	_, noCache1 := db.Get(NoCache)
	_, noCache := db.InstanceGet(NoCache)
	if db.Statement.Schema == nil || !StructHasIDField(db.Statement.ReflectValue) || noCache1 || noCache {
		//color.Yellow("no cache: %s", db.ToSQL(func(tx *gorm.DB) *gorm.DB {
		//	callbacks.BuildQuerySQL(tx)
		//	return tx
		//}))
		callbacks.Query(db)
		return
	}
	callbacks.BuildQuerySQL(db)
	sq := db.ToSQL(func(tx *gorm.DB) *gorm.DB { return tx })
	//color.Red("query db: %s", sq)
	data, err, _ := localSingleFlight.Do(sq, func() (interface{}, error) {
		var err error
		//color.Red("query db: %s", sq)
		db1 := db
		p.queryWithCache(db1)
		if db1.Error != nil {
			return nil, db1.Error
		}
		c := gm2call{RowsAffected: db1.RowsAffected}
		if c.RowsAffected > 0 && db1.Statement.Dest != nil {
			c.Data, err = json.Marshal(db1.Statement.Dest)
		}
		return c, err
	})
	db.Error = err
	if err != nil {
		return
	}
	c := data.(gm2call)
	db.RowsAffected = c.RowsAffected
	if len(c.Data) > 0 {
		err = json.Unmarshal(c.Data, db.Statement.Dest)
		if err != nil {
			_ = db.AddError(err)
			return
		}
	}
}

func (p *gormPluginCache) queryCachePk(stm *gorm.Statement, key string) (hit bool) {
	data, has := p.Get(key)
	if !has {
		atomic.AddUint64(&PCacheStat.PrimaryMiss, 1)
		return
	}
	if bytes.Equal(data, nullBytes) {
		hit = true
		stm.Error = gorm.ErrRecordNotFound
		return
	}
	err := json.Unmarshal(data, stm.Dest)
	if err != nil {
		_ = stm.AddError(err)
		return
	}
	return true
}

func (p *gormPluginCache) queryCacheSearch(stm *gorm.Statement, key string) (pk string, hit bool) {
	data, has := p.Get(key)
	if !has {
		atomic.AddUint64(&PCacheStat.SearchMiss, 1)
		return
	}
	//color.Green("queryCacheSearch cache get: %s, data:%s", key, string(data))
	if bytes.Equal(data, nullBytes) {
		hit = true
		stm.Error = gorm.ErrRecordNotFound
		return
	}
	// parse uint from bytes without unnecessary allocations
	s := strings.TrimSpace(string(data))
	pkIntVal, err := strconv.ParseUint(s, 10, 64)
	if err != nil || pkIntVal == 0 {
		return
	}
	pkKeyName := getPrimaryKeyName(stm)
	if pkKeyName == "" {
		return
	}
	hit = true
	pk = primaryCacheKey(p.Prefix, stm.Table, fmt.Sprintf("%s=%d", pkKeyName, pkIntVal))
	return
}

func (p *gormPluginCache) queryWithCache(db *gorm.DB) {
	if db.Error != nil {
		return
	}
	stm := db.Statement
	if stm.ReflectValue.Kind() != reflect.Struct || stm.Schema == nil {
		callbacks.Query(db)
		return
	}
	callbacks.BuildQuerySQL(db)
	hit := p.getCache(stm)
	//color.Green("hit: %v, sq:%s", hit, db.ToSQL(func(tx *gorm.DB) *gorm.DB { return tx }))
	if hit {
		if db.Error == nil {
			db.RowsAffected = 1
		}
		stm.SQL.Reset()
		stm.Vars = nil
		return
	}
	callbacks.Query(db)
	p.setCache(stm)
}

func (p *gormPluginCache) setCache(stm *gorm.Statement) {
	isNotFound := errors.Is(stm.Error, gorm.ErrRecordNotFound)
	key, isPrimary := p.getCacheKey(stm)
	//color.Red("setCache: %s, isPrimary:%v, isNotFound:%v", key, isPrimary, isNotFound)
	if !isPrimary {
		if isNotFound {
			p.Set(key, nullBytes, 60)
			return
		}
		idv, idStr := getPrimaryCacheKVFromDest(stm)
		if idStr == "" {
			return
		}
		p.Set(key, []byte(idv), p.TTL)
		key = primaryCacheKey(p.Prefix, stm.Table, idStr)
	}
	if isNotFound {
		p.Set(key, nullBytes, 60)
		return
	}
	data, err := json.Marshal(stm.Dest)
	if err != nil {
		_ = stm.AddError(err)
		return
	}
	p.Set(key, data, p.TTL)
}

func NewPlugin(c Gm2cConfig) gorm.Plugin {
	if c.Skip {
		return &gormPluginCache{Gm2cConfig: c}
	}
	if c.Prefix == "" {
		c.Prefix = "default"
	}
	if c.Store == nil {
		c.Store = GetFreeCacheStore()
	}

	if gc, ok := c.Store.(StoreGC); ok {
		tk := time.NewTicker(gcInterval)
		go func() {
			for range tk.C {
				gc.StoreGC(globalPrefix + ":" + c.Prefix)
			}
		}()
	}

	return &gormPluginCache{Gm2cConfig: c}
}

func StructHasIDField(v reflect.Value) bool {
	if !v.IsValid() {
		return false
	}
	if v.Kind() != reflect.Struct {
		return false
	}
	if v.FieldByName("ID").Kind() == reflect.Invalid {
		return false
	}
	return true
}

func clauseFindPrimaryCacheKey(expr clause.Expression, primaryDBName string) (str string) {
	var id string
	_ = expr
	switch ep := expr.(type) {
	case clause.IN:
		if ep.Column != clause.PrimaryColumn {
			return
		}
		if len(ep.Values) != 1 {
			return
		}
		id = cast.ToString(ep.Values[0])
	case clause.Eq:
		_column, _ok := ep.Column.(clause.Column)
		if !_ok {
			return
		}
		if _column.Name != primaryDBName {
			return
		}
		id = cast.ToString(ep.Value)
	case clause.Expr:
		sq := strings.TrimSpace(ep.SQL)
		if strings.Contains(sq, "?") && len(ep.Vars) == 1 {
			sq = strings.Replace(sq, "?", cast.ToString(ep.Vars[0]), -1)
		}
		sq = strings.Replace(sq, " ", "", -1)
		// use cached regexp to avoid recompilation
		var re *regexp.Regexp
		if v, ok := reRegexCache.Load(primaryDBName); ok {
			re = v.(*regexp.Regexp)
		} else {
			r := regexp.MustCompile(`^` + primaryDBName + `=\d+$`)
			actual, _ := reRegexCache.LoadOrStore(primaryDBName, r)
			re = actual.(*regexp.Regexp)
		}
		if !re.MatchString(sq) {
			return
		}
		sl1 := strings.Split(sq, "=")
		if len(sl1) != 2 {
			return
		}
		id = sl1[1]
	default:
		return
	}
	if id == "" {
		return
	}
	str = fmt.Sprintf("%s=%s", primaryDBName, id)
	return
}

func getPrimaryCacheKVFromDest(stm *gorm.Statement) (idv string, val string) {
	name := getPrimaryKeyName(stm)
	if name == "" {
		return
	}
	v := getPrimaryValue(stm)
	if v == nil {
		return
	}
	return cast.ToString(v), fmt.Sprintf("%s=%s", name, cast.ToString(v))
}

func getPrimaryKeyName(stm *gorm.Statement) (name string) {
	if stm.Schema == nil {
		return
	}
	fields := stm.Schema.PrimaryFields
	if len(fields) != 1 {
		return
	}
	return fields[0].DBName
}

func getPrimaryValue(stm *gorm.Statement) (val interface{}) {
	if stm.Schema == nil {
		return
	}
	fields := stm.Schema.PrimaryFields
	if len(fields) != 1 {
		return
	}
	field := fields[0]
	v, zero := field.ValueOf(context.Background(), stm.ReflectValue)
	if zero {
		return
	}
	val = v
	return
}

func primaryCacheKey(pre, table, id string) string {
	return fmt.Sprintf("%s%s%s", primaryKeyPrefix(pre, table), separator, id)
}

func primaryKeyPrefix(pre, table string) string {
	return fmt.Sprintf("%s:%s:%s:p", globalPrefix, pre, table)
}

func searchCacheKey(pre, table, sq string) string {
	return fmt.Sprintf("%s%s[%s]", searchKeyPrefix(pre, table), separator, sq)
}

func searchKeyPrefix(pre, table string) string {
	return fmt.Sprintf("%s:%s:%s:s", globalPrefix, pre, table)
}

func tablePrefix(pre, table string) string {
	return fmt.Sprintf("%s:%s:%s", globalPrefix, pre, table)
}
