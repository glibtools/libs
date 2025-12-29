package mdb

import (
	"bytes"
	"context"
	"errors"
	"reflect"
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

	"github.com/glibtools/libs/util"
)

const (
	NoCache = "no-cache"

	globalPrefix = "gm2c"
	separator    = ":sep:"
	nullValue    = "null"

	gcInterval = time.Second * 60 * 10
)
const stmtKeyCache = "sql_key_cache"

var (
	PCacheStat        = new(PluginCacheStat)
	_                 = color.Red
	localSingleFlight = singleflight.Group{}
	// reuse null byte slice to avoid repeated allocations when comparing/storing null marker
	nullBytes = []byte(nullValue)

	// cache reflect.Type.String() results for dest type in singleflight key
	destTypeStringCache sync.Map // map[reflect.Type]string
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

	// 原实现用 map 确保只出现 1 个不同的 pk 值；这里用常量空间等价实现
	found := false
	foundVal := ""
	var v string
	for _, expr := range where.Exprs {
		v = clauseFindPrimaryCacheKey(expr, primaryDBName)
		//color.Red("findPrimaryCacheKey expr: %#v, primaryDBName: %s, v: %s", expr, primaryDBName, v)
		if v == "" {
			continue
		}
		if justHas {
			return primaryCacheKey(p.Prefix, stm.Table, v)
		}
		if !found {
			found = true
			foundVal = v
			continue
		}
		if v != foundVal {
			// 多个不同 pk 值，与原 len(m)!=1 等价
			return ""
		}
	}
	if !found {
		return
	}
	return primaryCacheKey(p.Prefix, stm.Table, foundVal)
}

func (p *gormPluginCache) getCache(stm *gorm.Statement) (hit bool) {
	key, isPrimary := p.getCacheKey(stm)
	if key == "" {
		return
	}

	// 将统计合并到单次 atomic，避免 hot path 上重复原子操作
	var (
		searchChecked  bool
		searchHit      bool
		primaryChecked bool
		primaryHit     bool
	)

	if !isPrimary {
		// 只有确实会去 Get(searchKey) 时才标记 checked
		searchChecked = true
		var pKey string
		pKey, searchHit = p.queryCacheSearch(stm, key)
		key = pKey
	}
	if key != "" {
		// 只有确实会去 Get(primaryKey) 时才标记 checked
		primaryChecked = true
		primaryHit = p.queryCachePk(stm, key)
	}

	// 统一更新统计
	if searchChecked {
		if searchHit {
			atomic.AddUint64(&PCacheStat.SearchHit, 1)
			//color.Green("Hit search cache, key: %s", key)
		} else {
			atomic.AddUint64(&PCacheStat.SearchMiss, 1)
		}
	}
	if primaryChecked {
		if primaryHit {
			atomic.AddUint64(&PCacheStat.PrimaryHit, 1)
			//color.Green("Hit primary cache, key: %s", key)
		} else {
			atomic.AddUint64(&PCacheStat.PrimaryMiss, 1)
		}
	}

	return primaryHit
}

// getCacheKey 根据 statement 生成缓存 key。
// - 优先尝试从 WHERE 条件中提取主键等值 -> primary cache key
// - 否则使用稳定 SQL key -> search cache key
func (p *gormPluginCache) getCacheKey(stm *gorm.Statement) (key string, isPrimary bool) {
	pk := p.findPrimaryCacheKey(stm, true)
	if pk != "" {
		return pk, true
	}
	sq := sqlKeyFromStmt(stm)
	return searchCacheKey(p.Prefix, stm.Table, sq), false
}

func (p *gormPluginCache) query(db *gorm.DB) {
	if db.Error != nil {
		return
	}
	_, noCache1 := db.Get(NoCache)
	_, noCache := db.InstanceGet(NoCache)
	if db.Statement.Schema == nil || !StructHasIDField(db.Statement.ReflectValue) || noCache1 || noCache {
		callbacks.Query(db)
		return
	}

	// 构建 SQL，保证 cache key 稳定
	callbacks.BuildQuerySQL(db)

	// 1) fast path: 先只查缓存（不回源），命中则直接返回
	if p.queryCacheOnly(db, true) {
		return
	}

	// 2) miss 后才进入 singleflight 合并回源
	sfKey := singleFlightKey(p, db.Statement)

	// 说明：singleflight 的 leader 协程不应该直接复用外层请求的 db 实例去回源，
	// 因为 *gorm.DB / Statement 带有可变状态且不是跨 goroutine 共享设计。
	// 这里用 Session(NewDB:true) 创建独立 session 去执行回源，只负责回填缓存。
	_, err, _ := localSingleFlight.Do(sfKey, func() (interface{}, error) {
		// 双重检查：等待期间 可能已有别的请求填充缓存
		if p.queryCacheOnly(db, true) {
			return nil, nil
		}

		// 使用独立 session 回源：NewDB=true 避免复用/污染外层 Statement
		qdb := db.Session(&gorm.Session{NewDB: true, PrepareStmt: true, Logger: db.Logger})
		qdb.DryRun = false
		qdb.Error = nil
		qdb.Statement.Clauses = db.Statement.Clauses
		// 准备一个独立的 dest，避免污染外层请求
		reflectValue := reflect.New(reflect.TypeOf(db.Statement.Dest).Elem())
		qdb.Statement.ReflectValue = reflectValue.Elem()
		qdb.Statement.Dest = reflectValue.Interface()
		qdb.Model(qdb.Statement.Dest).Raw(qdb.Statement.SQL.String(), qdb.Statement.Vars...).Scan(qdb.Statement.Dest)
		p.setCache(qdb.Statement)
		return nil, nil
	})

	if err != nil {
		db.Error = err
		return
	}

	// singleflight 返回后（leader/等待者），缓存应当可用：统一从缓存回填结果
	// 注意：这里是 re-check，不会回源。
	if p.queryCacheOnly(db, false) {
		return
	}

	// 理论上不会走到这里；如果 Store 写入失败或被外界清理，fallback 到直连查询。
	callbacks.Query(db)
}

// queryCacheOnly 只尝试命中缓存，不会触发 callbacks.Query。
// 命中返回 true，并保证 db.RowsAffected/db.Error/db.Statement.Dest 状态与原命中路径一致。
// isFirst 参数表示是否为 第一次 查询，用于清理 SQL/Vars 状态。如果从数据库回源后再次调用则传 false。
func (p *gormPluginCache) queryCacheOnly(db *gorm.DB, isFirst bool) (hit bool) {
	if db == nil || db.Error != nil {
		return false
	}
	stm := db.Statement
	if stm == nil {
		return false
	}
	if stm.ReflectValue.Kind() != reflect.Struct || stm.Schema == nil {
		return false
	}
	if !p.getCache(stm) {
		return false
	}
	// 命中（包括 null marker）时，与原 queryWithCache 行为保持一致
	if db.Error == nil {
		db.RowsAffected = 1
	}
	if isFirst {
		stm.SQL.Reset()
		stm.Vars = nil
	}
	return true
}

func (p *gormPluginCache) queryCachePk(stm *gorm.Statement, key string) (hit bool) {
	data, has := p.Get(key)
	//color.Red("hasKey:%v, key:%s, data:%s", has, key, string(data))
	if !has {
		return
	}
	if len(data) == 0 {
		return
	}
	if bytes.Equal(data, nullBytes) {
		hit = true
		stm.Error = gorm.ErrRecordNotFound
		return
	}
	err := util.Unmarshal(data, stm.Dest)
	if err != nil {
		_ = stm.AddError(err)
		return
	}
	return true
}

func (p *gormPluginCache) queryCacheSearch(stm *gorm.Statement, key string) (pk string, hit bool) {
	data, has := p.Get(key)
	if !has {
		return
	}
	if len(data) == 0 {
		return
	}
	if bytes.Equal(data, nullBytes) {
		pk = ""
		hit = true
		stm.Error = gorm.ErrRecordNotFound
		return
	}

	// trim space then parse uint in-place (no string allocation);
	// keep semantics: decimal digits only, pk != 0; with overflow guard similar to strconv.ParseUint
	b := bytes.TrimSpace(data)
	if len(b) == 0 {
		return
	}
	var n uint64
	for i := 0; i < len(b); i++ {
		c := b[i]
		if c < '0' || c > '9' {
			return
		}
		d := uint64(c - '0')
		// overflow check: n*10 + d must not overflow uint64
		if n > (^(uint64(0))-d)/10 {
			return
		}
		n = n*10 + d
	}
	if n == 0 {
		return
	}

	pkKeyName := getPrimaryKeyName(stm)
	if pkKeyName == "" {
		return
	}
	hit = true
	// 保持原格式: "{pkName}={uint}"
	pk = primaryCacheKey(p.Prefix, stm.Table, pkKeyName+"="+strconv.FormatUint(n, 10))
	return
}

// queryWithCache 保持原行为：先查缓存，miss 后回源并写缓存。
// 其它业务如需该语义仍可使用。
func (p *gormPluginCache) queryWithCache(db *gorm.DB) {
	if db.Error != nil {
		return
	}
	stm := db.Statement
	if stm.ReflectValue.Kind() != reflect.Struct || stm.Schema == nil {
		callbacks.Query(db)
		return
	}
	hit := p.getCache(stm)
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
	if stm.Error != nil && !errors.Is(stm.Error, gorm.ErrRecordNotFound) {
		return
	}
	isNotFound := errors.Is(stm.Error, gorm.ErrRecordNotFound)
	key, isPrimary := p.getCacheKey(stm)
	//color.Red("setCache key: %s, isPrimary: %v, isNotFound: %v, error: %#+v", key, isPrimary, isNotFound, stm.Error)
	if !isPrimary {
		if isNotFound {
			p.Set(key, nullBytes, 60)
		}
		idv, idStr := getPrimaryCacheKVFromDest(stm)
		//color.Magenta("set cache primary key: %s, idStr: %s", key, idStr)
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
	data, err := util.Marshal(stm.Dest)
	//color.HiMagenta("set cache key: %s, dest: %s", key, util.JSONDump(stm.Dest))
	if err != nil {
		_ = stm.AddError(err)
		return
	}
	if len(data) == 0 {
		return
	}
	p.Set(key, data, p.TTL)
	//color.HiMagenta("set cache key: %s, dest: %s", key, util.JSONDump(stm.Dest))
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
	f := v.FieldByName("ID")
	return f.IsValid()
}

func clauseFindPrimaryCacheKey(expr clause.Expression, primaryDBName string) (str string) {
	var id string
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
		// 等价于原逻辑：
		// 1) TrimSpace(ep.SQL)
		sq := strings.TrimSpace(ep.SQL)

		// 2) 如果包含 '?' 且 len(Vars)==1，则用 Vars[0] 替换所有 '?'（原实现 Replace -1）
		// 3) 去掉所有空格，然后要求完全匹配 "{pk}={digits}"
		if strings.Contains(sq, "?") {
			if len(ep.Vars) != 1 {
				return
			}
			sq = strings.Replace(sq, "?", cast.ToString(ep.Vars[0]), -1)
		}

		//color.Red("clauseFindPrimaryCacheKey sq after replace: %s", sq)

		// 只处理简单等值表达式：lhs = rhs（允许 左右 空格）
		i := strings.IndexByte(sq, '=')
		if i <= 0 || i >= len(sq)-1 {
			return
		}

		lhs := strings.TrimSpace(sq[:i])
		rhs := strings.TrimSpace(sq[i+1:])

		if lhs != primaryDBName {
			return
		}
		if rhs == "" {
			return
		}
		for j := 0; j < len(rhs); j++ {
			c := rhs[j]
			if c < '0' || c > '9' {
				return
			}
		}
		id = rhs
	default:
		return
	}
	if id == "" {
		return
	}
	str = primaryDBName + "=" + id
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
	vs := cast.ToString(v)
	return vs, name + "=" + vs
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
	// fmt.Sprintf("%s%s%s", primaryKeyPrefix(pre, table), separator, id)
	return primaryKeyPrefix(pre, table) + separator + id
}

func primaryKeyPrefix(pre, table string) string {
	// fmt.Sprintf("%s:%s:%s:p", globalPrefix, pre, table)
	return globalPrefix + ":" + pre + ":" + table + ":p"
}

func searchCacheKey(pre, table, sq string) string {
	//color.Red("%s%s[%s]", searchKeyPrefix(pre, table), separator, sq)
	return searchKeyPrefix(pre, table) + separator + "[" + sq + "]"
}

func searchKeyPrefix(pre, table string) string {
	// fmt.Sprintf("%s:%s:%s:s", globalPrefix, pre, table)
	return globalPrefix + ":" + pre + ":" + table + ":s"
}

func singleFlightKey(p *gormPluginCache, stm *gorm.Statement) string {
	sq := sqlKeyFromStmt(stm)
	destTyp := ""
	table := ""
	if stm != nil {
		table = stm.Table
		if stm.Dest != nil {
			t := reflect.TypeOf(stm.Dest)
			if v, ok := destTypeStringCache.Load(t); ok {
				destTyp = v.(string)
			} else {
				s := t.String()
				actual, _ := destTypeStringCache.LoadOrStore(t, s)
				destTyp = actual.(string)
			}
		}
	}
	// fmt.Sprintf("%s:%s:%s:%s:%s", globalPrefix, p.Prefix, table, destTyp, sq)
	return globalPrefix + ":" + p.Prefix + ":" + table + ":" + destTyp + ":" + sq
}

func sqlKeyFromStmt(stm *gorm.Statement) string {
	if stm == nil || stm.DB == nil {
		return ""
	}
	if v, ok := stm.Settings.Load(stmtKeyCache); ok {
		// 防止异类型写入导致 panic
		if s, ok2 := v.(string); ok2 {
			return s
		}
	}
	// 若当前 SQL 还未构建，则构建一次
	if stm.SQL.Len() == 0 {
		callbacks.BuildQuerySQL(stm.DB)
	}

	// 性能优化：避免 Dialector.Explain 生成巨大 SQL 字符串。
	// 语义要求：不同 Vars 不能共享同一个 key。
	// 做法：key = SQL模板 + "\x00" + varsDigest(hex)。
	// Vars 为空时，SQL 模板本身已稳定。
	base := stm.SQL.String()
	var key string
	if len(stm.Vars) == 0 {
		key = base
	} else {
		d := varsDigest64(stm.Vars)
		key = base + "\x00" + digestKeySuffix(d)
	}

	stm.Settings.Store(stmtKeyCache, key)
	return key
}

func tablePrefix(pre, table string) string {
	// fmt.Sprintf("%s:%s:%s", globalPrefix, pre, table)
	return globalPrefix + ":" + pre + ":" + table
}
