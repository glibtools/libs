package mdb

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/fnv"
	"math"
	"reflect"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/fatih/color"
	"github.com/spf13/cast"
	"golang.org/x/sync/singleflight"
	"gorm.io/gorm"
	"gorm.io/gorm/callbacks"
	"gorm.io/gorm/clause"

	"github.com/glibtools/libs/util"
)

const (
	NoCache = "no-cache"

	globalPrefix = "gm2c"
	nullValue    = "__NULL__"

	// negative cache TTL (seconds), 60秒足够了
	negativeTTL = int64(60)
)

var (
	sfGroup   singleflight.Group
	nullBytes = []byte(nullValue)

	Gm2cCacheStat = Gm2cStats{}

	_ = color.Red
)

// CacheStore defines the cache storage interface.
/*
type ItfStorageCache interface {
	Get(key string) ([]byte, bool)
	Set(key string, val []byte, ttl ...int64) // ttl in seconds
	Del(key string)
	DropPrefix(prefix ...string)
}
*/
type CacheStore interface {
	ItfStorageCache
}

type Gm2cConfig struct {
	Skip   bool
	Prefix string
	TTL    int64 // seconds
	Store  CacheStore
}

func (g *Gm2cConfig) ClearTable(table string) {
	if g.Store == nil {
		return
	}
	g.Store.DropPrefix(tablePrefix(g.Prefix, table))
}

type Gm2cPlugin struct {
	cfg Gm2cConfig
}

func (p *Gm2cPlugin) Initialize(db *gorm.DB) error {
	_ = db.Callback().Query().Replace("gorm:query", p.query)
	_ = db.Callback().Create().After("gorm:create").Register(globalPrefix+":cache:invalidate", p.invalidate)
	_ = db.Callback().Update().After("gorm:update").Register(globalPrefix+":cache:invalidate", p.invalidate)
	_ = db.Callback().Delete().After("gorm:delete").Register(globalPrefix+":cache:invalidate", p.invalidate)
	return nil
}

func (p *Gm2cPlugin) Name() string { return "single-record-cache" }

// cacheKey:
// - If WHERE contains single pk equality: use primary cache key
// - Else: use search cache key (SQL template + vars digest)
func (p *Gm2cPlugin) cacheKey(stm *gorm.Statement) (key string, isPrimary bool) {
	if pk := primaryFromWhere(stm); pk != "" {
		return primaryKey(p.cfg.Prefix, stm.Table, pk), true
	}
	return searchKey(p.cfg.Prefix, stm.Table, searchUnionKey(stm)), false
}

func (p *Gm2cPlugin) invalidate(db *gorm.DB) {
	stm := db.Statement
	if stm == nil || stm.Schema == nil || stm.Table == "" || p.cfg.Store == nil || p.cfg.Skip {
		return
	}
	if skipInvalidate(db.Statement) {
		return
	}

	// Always drop search cache for this table: any create/update/delete may affect unique lookup queries.
	p.cfg.Store.DropPrefix(searchPrefix(p.cfg.Prefix, stm.Table))

	if stm.ReflectValue.Kind() != reflect.Struct {
		p.cfg.Store.DropPrefix(primaryPrefix(p.cfg.Prefix, stm.Table))
		return
	}

	// Try to precisely delete one primary cache entry if we can find a single pk value.
	// Fallback: drop all primary cache for this table.
	var pk string
	if stm.Schema != nil && len(stm.Schema.PrimaryFields) == 1 {
		// 1) From WHERE (e.g. UPDATE ... WHERE id=1)
		pk = primaryFromWhere(stm)
		// 2) From dest struct (e.g. Create/Delete with model loaded)
		if pk == "" {
			pk = primaryValue(stm)
		}
	}
	if pk != "" {
		p.cfg.Store.Del(primaryKey(p.cfg.Prefix, stm.Table, pk))
		return
	}
	p.cfg.Store.DropPrefix(primaryPrefix(p.cfg.Prefix, stm.Table))
}

func (p *Gm2cPlugin) query(db *gorm.DB) {
	if db.Error != nil {
		return
	}
	if skipCache(db.Statement) || p.cfg.Skip || p.cfg.Store == nil {
		callbacks.Query(db)
		return
	}

	// Ensure SQL/VARS are built before generating keys.
	callbacks.BuildQuerySQL(db)

	// 1) cache-only fast path
	if p.tryCache(db, false) {
		atomic.AddUint64(&Gm2cCacheStat.Hit, 1)
		return
	}

	atomic.AddUint64(&Gm2cCacheStat.Miss, 1)

	// 2) singleflight fetch
	sfKey := singleFlightKey(p.cfg.Prefix, db.Statement)
	_, err, _ := sfGroup.Do(sfKey, func() (any, error) {
		innerDB := db.Session(&gorm.Session{NewDB: true, Logger: db.Logger})
		// double-check cache
		if p.tryCache(innerDB, false) {
			return nil, nil
		}

		callbacks.Query(innerDB)

		// ⭐ 同步错误（非常关键）
		//将错误传给statement，方便setCache使用;防止两遍 error 不一致
		//为什么这里的 innerDB.Error!=nil 时,但是 stm.Error==nil ?
		innerDB.Statement.Error = innerDB.Error

		p.setCache(innerDB.Statement)
		return nil, innerDB.Error
	})

	// if db.Error is not nil here, it must be gorm.ErrRecordNotFound or real error
	// (not other kinds of errors, which should have been returned by singleflight)
	// so that we don't overwrite it.
	if err != nil {
		db.Error = err
		return
	}

	gotCache := p.tryCache(db, true)
	noDBError := db.Error == nil
	if !gotCache && noDBError {
		// 理论上不该发生
		// 可以选择 fallback：
		callbacks.Query(db)
	}
}

// setCache:
// - if NotFound: write NULL to whichever key we used (primary OR search)
// - if Found:
//   - always write primary object cache
//   - if key is search (non-primary), write search->pk mapping
func (p *Gm2cPlugin) setCache(stm *gorm.Statement) {
	if stm.Error != nil && !errors.Is(stm.Error, gorm.ErrRecordNotFound) {
		return
	}

	key, isPrimary := p.cacheKey(stm)
	if key == "" {
		return
	}

	// NotFound must be cached and must not force caller to re-query DB next time.
	if errors.Is(stm.Error, gorm.ErrRecordNotFound) {
		p.cfg.Store.Set(key, nullBytes, negativeTTL)
		return
	}

	// Extract PK from loaded struct
	pkVal := primaryValue(stm)
	if pkVal == "" {
		return
	}

	// Write primary object cache
	obj, err := util.Marshal(stm.Dest)
	if err != nil || len(obj) == 0 {
		return
	}
	pkKey := primaryKey(p.cfg.Prefix, stm.Table, pkVal)
	p.cfg.Store.Set(pkKey, obj, p.cfg.TTL)

	// For search-key queries: map search -> pk (NOT object)
	if !isPrimary {
		p.cfg.Store.Set(key, []byte(pkVal), p.cfg.TTL)
	}
}

// tryCache:
// - If primary key query: primary cache stores object/NULL
// - If non-primary query: search cache stores "pk" or NULL; then load primary cache.
func (p *Gm2cPlugin) tryCache(db *gorm.DB, withVars bool) bool {
	stm := db.Statement
	key, isPrimary := p.cacheKey(stm)
	if key == "" {
		return false
	}

	// Primary cache: object or NULL
	if isPrimary {
		return p.writeCacheResult(db, key, withVars)
	}

	// Search cache: pk string or NULL
	data, ok := p.cfg.Store.Get(key)
	if !ok {
		return false
	}
	if bytes.Equal(data, nullBytes) {
		// NotFound must be served purely from cache
		return p.writeNotFound(db, withVars)
	}
	pk := string(data)
	if pk == "" {
		return false
	}
	return p.writeCacheResult(db, primaryKey(p.cfg.Prefix, stm.Table, pk), withVars)
}

func (p *Gm2cPlugin) writeCacheResult(db *gorm.DB, key string, withVars bool) bool {
	data, ok := p.cfg.Store.Get(key)
	if !ok {
		return false
	}
	if bytes.Equal(data, nullBytes) {
		return p.writeNotFound(db, withVars)
	}

	if err := util.Unmarshal(data, db.Statement.Dest); err != nil {
		return false
	}
	db.RowsAffected = 1
	if !withVars {
		// clean up SQL/VARS to avoid confusion
		// 清除了这些,后续就不会出现日志
		db.Statement.SQL.Reset()
		db.Statement.Vars = nil
	}
	return true
}

func (p *Gm2cPlugin) writeNotFound(db *gorm.DB, withVars bool) bool {
	db.Error = gorm.ErrRecordNotFound
	db.Statement.Error = db.Error
	db.RowsAffected = 0
	if !withVars {
		db.Statement.SQL.Reset()
		db.Statement.Vars = nil
	}
	return true
}

type Gm2cStats struct {
	Hit  uint64 `json:"hit"`
	Miss uint64 `json:"miss"`
}

func NewPlugin(cfg Gm2cConfig) gorm.Plugin {
	if cfg.Prefix == "" {
		cfg.Prefix = globalPrefix
	}
	return &Gm2cPlugin{cfg: cfg}
}

func extractPkFromExpr(e clause.Expr, pkName string) string {
	sql := strings.TrimSpace(e.SQL)
	if sql == "" {
		return ""
	}

	// 只允许一个 "="
	idx := strings.IndexByte(sql, '=')
	if idx <= 0 || idx >= len(sql)-1 {
		return ""
	}

	left := strings.TrimSpace(sql[:idx])
	right := strings.TrimSpace(sql[idx+1:])

	// 去掉字段名可能的引号
	left = strings.Trim(left, "`\"")

	if left != pkName {
		return ""
	}

	// 情况 1：id = ?
	if right == "?" {
		if len(e.Vars) != 1 {
			return ""
		}
		return cast.ToString(e.Vars[0])
	}

	// 情况 2：id = 6（只接受纯数字）
	for i := 0; i < len(right); i++ {
		c := right[i]
		if c < '0' || c > '9' {
			return ""
		}
	}

	return right
}

func hasIN(stm *gorm.Statement) bool {
	cs, ok := stm.Clauses["WHERE"]
	if !ok {
		return false
	}
	w, ok := cs.Expression.(clause.Where)
	if !ok {
		return false
	}
	for _, e := range w.Exprs {
		if ep, isIN := e.(clause.IN); isIN && len(ep.Values) != 1 {
			return true
		}
	}
	return false
}

func notLimit1(stm *gorm.Statement) bool {
	lim, ok := stm.Clauses["LIMIT"]
	if !ok {
		return true
	}
	l, ok := lim.Expression.(clause.Limit)
	return ok && l.Limit != nil && *l.Limit != 1
}

// primaryFromWhere returns pk value if and only if WHERE is exactly pk equality (or multiple identical pk equalities).
// Any other WHERE shape returns "" to force search cache key path.
func primaryFromWhere(stm *gorm.Statement) string {
	if stm == nil || stm.Schema == nil || len(stm.Schema.PrimaryFields) != 1 {
		return ""
	}

	cs, ok := stm.Clauses["WHERE"]
	if !ok {
		return ""
	}

	where, ok := cs.Expression.(clause.Where)
	if !ok || len(where.Exprs) == 0 {
		return ""
	}

	pkName := stm.Schema.PrimaryFields[0].DBName
	var found string

	setFound := func(v string) bool {
		if v == "" {
			return false
		}
		if found == "" {
			found = v
			return true
		}
		return found == v
	}

	isPkColumn := func(c any) bool {
		col, colOK := c.(clause.Column)
		if !colOK {
			return false
		}
		// 只比较 Name，忽略 Table/Alias
		return col.Name == pkName
	}

	for _, expr := range where.Exprs {
		switch e := expr.(type) {
		case clause.Eq:
			if !isPkColumn(e.Column) || !setFound(cast.ToString(e.Value)) {
				return ""
			}
		case clause.IN:
			//可能会出现找不到主键名
			/**
			expr: clause.IN{Column:clause.Column{Table:"~~~ct~~~", Name:"~~~py~~~", Alias:"", Raw:false}, Values:[]interface {}{1}}, type=clause.IN
			这种情况只能退化为搜索缓存
			*/
			if !isPkColumn(e.Column) || len(e.Values) != 1 || !setFound(cast.ToString(e.Values[0])) {
				return ""
			}
		case clause.Expr:
			// 只允许 "id = ?" 或 "id = 123" 这种形态
			if !setFound(extractPkFromExpr(e, pkName)) {
				return ""
			}
		default:
			return ""
		}
	}

	return found
}

func primaryKey(prefix, table, id string) string {
	// gm2c:<prefix>:<table>:p:<id>
	return tablePrefix(prefix, table) + "p:" + id
}

func primaryPrefix(prefix, table string) string {
	// gm2c:<prefix>:<table>:p:
	return tablePrefix(prefix, table) + "p:"
}

func primaryValue(stm *gorm.Statement) string {
	if stm == nil || stm.Schema == nil || len(stm.Schema.PrimaryFields) != 1 {
		return ""
	}
	f := stm.Schema.PrimaryFields[0]
	v, zero := f.ValueOf(context.Background(), stm.ReflectValue)
	if zero {
		return ""
	}
	return cast.ToString(v)
}

func searchKey(prefix, table, union string) string {
	// gm2c:<prefix>:<table>:s:<union>
	return tablePrefix(prefix, table) + "s:" + union
}

func searchPrefix(prefix, table string) string {
	// gm2c:<prefix>:<table>:s:
	return tablePrefix(prefix, table) + "s:"
}

// searchUnionKey makes SQL+vars stable:
// union = SQL_TEMPLATE + "\x00" + hex(varsDigest64(Vars))
func searchUnionKey(stm *gorm.Statement) string {
	base := ""
	if stm != nil {
		base = stm.SQL.String()
	}
	if stm == nil || len(stm.Vars) == 0 {
		return base
	}
	d := varsDigest64(stm.Vars)
	return base + "\x00" + varsDigestKeySuffix(d)
}

func singleFlightKey(prefix string, stm *gorm.Statement) string {
	if stm == nil {
		return globalPrefix + ":" + prefix + ":<nil>"
	}
	destType := "destTypeNil"
	if stm.Dest != nil {
		destType = reflect.TypeOf(stm.Dest).String()
	}
	union := searchUnionKey(stm)
	// gm2c:<prefix>:sf:<table>:<destType>:<union>
	resultKey := globalPrefix + ":" + prefix + ":sf:" + stm.Table + ":" + destType + ":" + union
	return util.MD5(resultKey)
}

func skipCache(stm *gorm.Statement) bool {
	if stm == nil || stm.Schema == nil {
		return true
	}
	// allow caller to disable caching
	if _, ok := stm.Get(NoCache); ok {
		return true
	}
	if _, ok := stm.InstanceGet(NoCache); ok {
		return true
	}
	// must be a struct single-record destination
	if stm.ReflectValue.Kind() != reflect.Struct {
		return true
	}
	// single primary key only
	if len(stm.Schema.PrimaryFields) != 1 {
		return true
	}
	// IN query must not be cached
	if hasIN(stm) {
		return true
	}
	// only LIMIT 1 is allowed, 如果不包含 LIMIT 则视为不走缓存,如果 LIMIT 不是 1 也不走缓存
	//只针对 Take/First/Last,或者手动设置 LIMIT 1 的场景走缓存
	if notLimit1(stm) {
		return true
	}
	return false
}

func skipInvalidate(stm *gorm.Statement) bool {
	if stm == nil || stm.Schema == nil {
		return true
	}
	// allow caller to disable cache invalidation
	if _, ok := stm.Get(NoCache); ok {
		return true
	}
	if _, ok := stm.InstanceGet(NoCache); ok {
		return true
	}
	if len(stm.Schema.PrimaryFields) != 1 {
		return true
	}
	return false
}

func tablePrefix(prefix, table string) string {
	// gm2c:<prefix>:<table>:
	return globalPrefix + ":" + prefix + ":" + table + ":"
}

func varsDigest64(vars []interface{}) uint64 {
	h := fnv.New64a()
	// include length to reduce collisions for different slice boundaries
	_ = binary.Write(h, binary.LittleEndian, uint64(len(vars)))

	var b8 [8]byte
	for _, v := range vars {
		if v == nil {
			_, _ = h.Write([]byte{0})
			_, _ = h.Write([]byte{0xFF})
			continue
		}
		rv := reflect.ValueOf(v)
		// unwrap pointers
		for rv.Kind() == reflect.Pointer {
			if rv.IsNil() {
				_, _ = h.Write([]byte{0})
				_, _ = h.Write([]byte{0xFF})
				goto next
			}
			rv = rv.Elem()
		}

		// special-case time.Time (and *time.Time via pointer unwrap above)
		if t, ok := rv.Interface().(time.Time); ok {
			_, _ = h.Write([]byte{7})
			binary.LittleEndian.PutUint64(b8[:], uint64(t.UnixNano()))
			_, _ = h.Write(b8[:])
			_, _ = h.Write([]byte{0xFF})
			continue
		}

		switch rv.Kind() {
		case reflect.Bool:
			_, _ = h.Write([]byte{1})
			if rv.Bool() {
				_, _ = h.Write([]byte{1})
			} else {
				_, _ = h.Write([]byte{0})
			}
		case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
			_, _ = h.Write([]byte{2})
			binary.LittleEndian.PutUint64(b8[:], uint64(rv.Int()))
			_, _ = h.Write(b8[:])
		case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr:
			_, _ = h.Write([]byte{3})
			binary.LittleEndian.PutUint64(b8[:], rv.Uint())
			_, _ = h.Write(b8[:])
		case reflect.Float32, reflect.Float64:
			_, _ = h.Write([]byte{4})
			binary.LittleEndian.PutUint64(b8[:], math.Float64bits(rv.Convert(reflect.TypeOf(float64(0))).Float()))
			_, _ = h.Write(b8[:])
		case reflect.String:
			_, _ = h.Write([]byte{5})
			s := rv.String()
			binary.LittleEndian.PutUint64(b8[:], uint64(len(s)))
			_, _ = h.Write(b8[:])
			_, _ = h.Write([]byte(s))
		case reflect.Slice:
			// common: []byte
			if rv.Type().Elem().Kind() == reflect.Uint8 {
				_, _ = h.Write([]byte{6})
				bs := rv.Bytes()
				binary.LittleEndian.PutUint64(b8[:], uint64(len(bs)))
				_, _ = h.Write(b8[:])
				_, _ = h.Write(bs)
				break
			}
			fallthrough
		default:
			// last resort: stable string representation
			_, _ = h.Write([]byte{8})
			s := fmt.Sprint(rv.Interface())
			binary.LittleEndian.PutUint64(b8[:], uint64(len(s)))
			_, _ = h.Write(b8[:])
			_, _ = h.Write([]byte(s))
		}

		_, _ = h.Write([]byte{0xFF})

	next:
	}
	return h.Sum64()
}

// varsDigest64 produces a stable digest for query vars so that different vars never share key.
// It avoids huge SQL strings and keeps semantics: SQL template + varsDigest.
func varsDigestKeySuffix(d uint64) string {
	// hex is slightly shorter than base10 and stable
	return strconv.FormatUint(d, 16)
}
