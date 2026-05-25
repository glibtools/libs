package gormcache

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"sync/atomic"

	"golang.org/x/sync/singleflight"
	"gorm.io/gorm"
	"gorm.io/gorm/callbacks"
	"gorm.io/gorm/clause"
)

const (
	defaultPrefix             = "gormcache"
	defaultTTLSeconds         = 600
	defaultNegativeTTLSeconds = 60
	// defaultStoreSize is the default bounded value-cache capacity. Production
	// deployments should size StoreSize for their working set and largest cached
	// row because freecache rejects entries larger than 1/1024 of its capacity.
	defaultStoreSize = 64 << 20
	// minFreeCacheStoreSize matches freecache's minimum practical capacity and
	// keeps derived epoch stores usable when callers choose tiny value stores.
	minFreeCacheStoreSize = 512 << 10
	nullMarker            = "__GORMCACHE_NULL__"
	// NoCache disables this plugin for the current GORM statement.
	NoCache = "gormcache:no-cache"
)

// NoCacheClause disables this plugin through GORM's clause path.
var NoCacheClause clause.Expression = noCacheClause{}
var nullMarkerBytes = []byte(nullMarker)

// Config controls Plugin behavior.
type Config struct {
	Prefix string
	// Disabled starts the plugin with query caching turned off.
	Disabled bool
	Store    Store
	// StoreSize is the value-cache capacity in bytes when Store is nil.
	// Zero uses the package default.
	StoreSize int
	// EpochStore stores table, row, global, and sweep invalidation tokens.
	// Share one EpochStore across plugin instances that must observe the same
	// invalidations; when nil, New creates a dedicated FreeCacheStore.
	EpochStore Store
	// EpochStoreSize is the epoch-cache capacity in bytes when EpochStore is nil.
	// Zero derives the capacity from StoreSize.
	EpochStoreSize int
	// TTLSeconds is the positive-result cache lifetime in seconds. Zero uses
	// the package default.
	TTLSeconds int64
	// NegativeTTLSeconds is the not-found cache lifetime in seconds. Zero uses
	// the package default.
	NegativeTTLSeconds int64
	Marshal            func(any) ([]byte, error)
	Unmarshal          func([]byte, any) error
}

// Plugin caches single-row GORM query results.
type Plugin struct {
	cfg           Config
	epochs        *tableEpochs
	group         singleflight.Group
	hits          atomic.Uint64
	misses        atomic.Uint64
	invalidations atomic.Uint64
	enabled       atomic.Bool
	beforeQuery   func(*gorm.DB)
	beforeFill    func(*gorm.DB)
}

// Disable turns off query cache reads and fills for this plugin instance.
func (p *Plugin) Disable() {
	p.enabled.Store(false)
}

// Enable turns on query cache reads and fills for this plugin instance.
func (p *Plugin) Enable() {
	p.enabled.Store(true)
}

// Enabled reports whether query caching is currently enabled.
func (p *Plugin) Enabled() bool {
	return p.enabled.Load()
}

// Initialize registers query and invalidation callbacks.
func (p *Plugin) Initialize(db *gorm.DB) error {
	return errors.Join(
		db.Callback().Query().Replace("gorm:query", p.query),
		db.Callback().Create().After("gorm:commit_or_rollback_transaction").Register(p.callbackName("create"), p.invalidate),
		db.Callback().Update().After("gorm:commit_or_rollback_transaction").Register(p.callbackName("update"), p.invalidate),
		db.Callback().Delete().After("gorm:commit_or_rollback_transaction").Register(p.callbackName("delete"), p.invalidate),
		db.Callback().Raw().After("gorm:raw").Register(p.callbackName("raw"), p.invalidateRaw),
	)
}

// InvalidateAll invalidates every cached entry observed by this plugin.
func (p *Plugin) InvalidateAll() {
	p.invalidations.Add(1)
	p.invalidateAll()
}

// InvalidateRow invalidates one table row by primary-key value.
func (p *Plugin) InvalidateRow(table, pkValue string) {
	p.invalidations.Add(1)
	p.invalidateRow(table, pkValue)
}

// InvalidateTable invalidates cached entries for table.
func (p *Plugin) InvalidateTable(table string) {
	p.invalidations.Add(1)
	p.invalidateTable(table)
}

// Name returns the GORM plugin name.
func (p *Plugin) Name() string {
	return "gormcache"
}

// Stats returns a snapshot of cache counters.
func (p *Plugin) Stats() Stats {
	stats := Stats{
		Hits:          p.hits.Load(),
		Misses:        p.misses.Load(),
		Invalidations: p.invalidations.Load(),
	}
	if s, ok := p.cfg.Store.(interface{ SetErrors() uint64 }); ok {
		stats.StoreSetErrors = s.SetErrors()
	}
	if s, ok := p.cfg.Store.(interface{ Evictions() uint64 }); ok {
		stats.StoreEvictions = s.Evictions()
	}
	return stats
}

func (p *Plugin) callbackName(name string) string {
	return p.cfg.Prefix + ":gormcache:" + name
}

func (p *Plugin) fillCache(db *gorm.DB, key cacheKey) flightResult {
	if p.beforeFill != nil {
		p.beforeFill(db)
	}
	stm := db.Statement
	if stm == nil {
		return flightResult{}
	}
	if errors.Is(stm.Error, gorm.ErrRecordNotFound) {
		shareable := p.validKeyToken(key)
		if shareable {
			p.cfg.Store.Set(key.key, []byte(nullMarker), p.cfg.NegativeTTLSeconds)
		}
		return flightResult{notFound: true, shareable: shareable}
	}
	if stm.Error != nil || db.Error != nil {
		return flightResult{}
	}

	pk := primaryValue(stm)
	if pk == "" {
		return flightResult{}
	}
	obj, err := p.cfg.Marshal(stm.Dest)
	if err != nil || len(obj) == 0 {
		return flightResult{}
	}
	shareable := p.validKeyToken(key)
	if !shareable {
		return flightResult{data: obj}
	}

	if key.isPrimary {
		p.cfg.Store.Set(key.key, obj, p.cfg.TTLSeconds)
		return flightResult{data: obj, shareable: true}
	}

	row := p.epochs.snapshotRow(key.table, pk, key.tableToken.global)
	primaryKey := buildPrimaryKey(p.cfg.Prefix, row)
	p.cfg.Store.Set(primaryKey, obj, p.cfg.TTLSeconds)
	p.cfg.Store.Set(key.key, []byte(pk), p.cfg.TTLSeconds)
	if !p.epochs.validRow(row) {
		p.cfg.Store.Del(primaryKey)
		return flightResult{data: obj}
	}
	return flightResult{data: obj, shareable: true}
}

func (p *Plugin) invalidate(db *gorm.DB) {
	// Keep invalidations active while query caching is disabled. Writes during a
	// disabled window must still bump epochs so re-enabling cannot expose stale
	// cache entries created before the switch was flipped.
	stm := db.Statement
	if stm == nil || stm.Schema == nil || stm.Table == "" {
		return
	}
	if db.Error != nil {
		return
	}
	if skipInvalidateStatement(stm) {
		return
	}
	pk := primaryFromWhere(stm)
	if pk == "" {
		pk = primaryValue(stm)
	}
	if pk != "" {
		p.invalidations.Add(1)
		p.invalidateRow(stm.Table, pk)
		return
	}
	p.invalidations.Add(1)
	p.invalidateTable(stm.Table)
}

func (p *Plugin) invalidateAll() {
	p.epochs.bumpGlobal()
}

func (p *Plugin) invalidateRaw(db *gorm.DB) {
	// Raw invalidation also remains active while disabled for the same
	// re-enable safety reason as the model callbacks above.
	if db == nil || db.Statement == nil {
		return
	}
	if db.Error != nil {
		return
	}
	if !isMutatingSQL(db.Statement.SQL.String()) {
		return
	}
	p.invalidations.Add(1)
	p.invalidateAll()
}

func (p *Plugin) invalidateRow(table, pk string) {
	if table == "" || pk == "" {
		p.epochs.bumpRow(table, pk)
		return
	}
	row := p.epochs.snapshotRow(table, pk, p.epochs.loadGlobal())
	oldKey := buildPrimaryKey(p.cfg.Prefix, row)
	p.epochs.bumpRow(table, pk)
	p.cfg.Store.Del(oldKey)
}

func (p *Plugin) invalidateTable(table string) {
	p.epochs.bumpTable(table)
}

func (p *Plugin) query(db *gorm.DB) {
	if db.Error != nil {
		return
	}
	stm := db.Statement
	if !p.Enabled() {
		callbacks.Query(db)
		return
	}
	if skipStatement(stm) {
		callbacks.Query(db)
		return
	}

	callbacks.BuildQuerySQL(db)
	if skipAfterSQLBuild(stm) {
		callbacks.Query(db)
		return
	}

	token := p.epochs.snapshotTable(stm.Table)
	key := keyForStatement(p.cfg.Prefix, p.epochs, token, stm, "")
	if p.tryCache(db, key) {
		p.hits.Add(1)
		return
	}
	p.misses.Add(1)

	digest := key.digest
	if digest == "" {
		digest = digestStatement(stm)
	}
	result, err, shared := p.group.Do(buildSingleflightKey(p.cfg.Prefix, stm, digest), func() (any, error) {
		if p.beforeQuery != nil {
			p.beforeQuery(db)
		}
		callbacks.Query(db)
		db.Statement.Error = db.Error
		return p.fillCache(db, key), nil
	})
	if err != nil {
		db.Error = err
		db.Statement.Error = err
		return
	}
	if shared {
		res := result.(flightResult)
		if res.shareable && p.writeFlightResult(db, res) {
			return
		}
		freshToken := p.epochs.snapshotTable(stm.Table)
		freshKey := keyForStatement(p.cfg.Prefix, p.epochs, freshToken, stm, digest)
		if p.tryCache(db, freshKey) {
			p.hits.Add(1)
			return
		}
		p.retrySharedMiss(db, stm, freshKey, digest)
	}
}

func (p *Plugin) retrySharedMiss(db *gorm.DB, stm *gorm.Statement, key cacheKey, digest string) {
	result, err, shared := p.group.Do(buildSingleflightKey(p.cfg.Prefix, stm, digest)+":retry", func() (any, error) {
		if p.beforeQuery != nil {
			p.beforeQuery(db)
		}
		callbacks.Query(db)
		db.Statement.Error = db.Error
		return p.fillCache(db, key), nil
	})
	if err != nil {
		db.Error = err
		db.Statement.Error = err
		return
	}
	if !shared {
		return
	}
	res := result.(flightResult)
	if res.shareable && p.writeFlightResult(db, res) {
		return
	}
	freshToken := p.epochs.snapshotTable(stm.Table)
	freshKey := keyForStatement(p.cfg.Prefix, p.epochs, freshToken, stm, digest)
	if p.tryCache(db, freshKey) {
		p.hits.Add(1)
		return
	}
	callbacks.Query(db)
	db.Statement.Error = db.Error
	_ = p.fillCache(db, freshKey)
}

func (p *Plugin) tryCache(db *gorm.DB, key cacheKey) bool {
	data, ok := p.cfg.Store.Get(key.key)
	if !ok {
		return false
	}
	if bytes.Equal(data, nullMarkerBytes) {
		return p.writeNotFound(db)
	}
	if key.isPrimary {
		return p.writeObject(db, data)
	}

	pk := string(data)
	if pk == "" {
		return false
	}
	token := p.epochs.snapshotRow(key.table, pk, key.tableToken.global)
	primaryKey := cacheKey{
		table:     key.table,
		key:       buildPrimaryKey(p.cfg.Prefix, token),
		pk:        pk,
		digest:    key.digest,
		isPrimary: true,
		rowToken:  token,
	}
	return p.tryCache(db, primaryKey)
}

func (p *Plugin) validKeyToken(key cacheKey) bool {
	if key.isPrimary {
		return p.epochs.validRow(key.rowToken)
	}
	return p.epochs.validTable(key.tableToken)
}

func (p *Plugin) writeFlightResult(db *gorm.DB, res flightResult) bool {
	if res.notFound {
		return p.writeNotFound(db)
	}
	if len(res.data) == 0 {
		return false
	}
	return p.writeObject(db, res.data)
}

func (p *Plugin) writeNotFound(db *gorm.DB) bool {
	db.Error = gorm.ErrRecordNotFound
	db.Statement.Error = db.Error
	db.RowsAffected = 0
	db.Statement.SQL.Reset()
	db.Statement.Vars = nil
	return true
}

func (p *Plugin) writeObject(db *gorm.DB, data []byte) bool {
	if err := p.cfg.Unmarshal(data, db.Statement.Dest); err != nil {
		return false
	}
	db.RowsAffected = 1
	db.Statement.SQL.Reset()
	db.Statement.Vars = nil
	return true
}

// Stats reports cache counters and backend health metrics.
type Stats struct {
	// Hits is the number of cache hits served by the plugin.
	Hits uint64
	// Misses is the number of cache misses observed by the plugin.
	Misses uint64
	// Invalidations is the number of invalidation events observed by the plugin.
	Invalidations uint64
	// StoreSetErrors is the number of value-store writes rejected by the backend.
	StoreSetErrors uint64
	// StoreEvictions is the number of value-store evictions reported by the backend.
	StoreEvictions uint64
}

type flightResult struct {
	data      []byte
	notFound  bool
	shareable bool
}

type noCacheClause struct{}

func (noCacheClause) Build(clause.Builder) {}

func (noCacheClause) MergeClause(*clause.Clause) {}

func (noCacheClause) Name() string { return NoCache }

// Install creates and registers a Plugin on db.
func Install(db *gorm.DB, cfg Config) (*Plugin, error) {
	plugin := New(cfg)
	if err := db.Use(plugin); err != nil {
		return nil, err
	}
	return plugin, nil
}

// New creates a Plugin from cfg.
func New(cfg Config) *Plugin {
	if cfg.Prefix == "" {
		cfg.Prefix = defaultPrefix
	}
	valueSize := cfg.StoreSize
	if valueSize <= 0 {
		valueSize = defaultStoreSize
	}
	cfg.StoreSize = valueSize
	if cfg.Store == nil {
		cfg.Store = NewFreeCacheStore(valueSize)
	}
	epochSize := cfg.EpochStoreSize
	if epochSize <= 0 {
		epochSize = derivedEpochStoreSize(valueSize)
	}
	cfg.EpochStoreSize = epochSize
	if cfg.EpochStore == nil {
		cfg.EpochStore = NewFreeCacheStore(epochSize)
	}
	if cfg.TTLSeconds <= 0 {
		cfg.TTLSeconds = defaultTTLSeconds
	}
	if cfg.NegativeTTLSeconds <= 0 {
		cfg.NegativeTTLSeconds = defaultNegativeTTLSeconds
	}
	if cfg.Marshal == nil {
		cfg.Marshal = json.Marshal
	}
	if cfg.Unmarshal == nil {
		cfg.Unmarshal = json.Unmarshal
	}
	plugin := &Plugin{cfg: cfg, epochs: newTableEpochs(cfg.Prefix, cfg.EpochStore)}
	plugin.enabled.Store(!cfg.Disabled)
	return plugin
}

func cteFinalToken(sql string) (string, bool) {
	sql = strings.TrimSpace(sql)
	if token, rest := firstSQLToken(sql); token == "RECURSIVE" {
		sql = strings.TrimSpace(rest)
	}
	for {
		open := indexCTEBody(sql)
		if open < 0 {
			return "WITH", true
		}
		closeIdx := matchingParen(sql[open:])
		if closeIdx < 0 {
			return "WITH", true
		}
		if isMutatingSQL(sql[open+1 : open+closeIdx]) {
			return "WITH", true
		}
		sql = strings.TrimSpace(sql[open+closeIdx+1:])
		if strings.HasPrefix(sql, ",") {
			sql = strings.TrimSpace(sql[1:])
			continue
		}
		token, _ := firstSQLToken(sql)
		if token == "" {
			return "WITH", true
		}
		return token, false
	}
}

func derivedEpochStoreSize(valueSize int) int {
	size := valueSize / 4
	if size < minFreeCacheStoreSize {
		return minFreeCacheStoreSize
	}
	return size
}

func firstSQLToken(sql string) (string, string) {
	sql = strings.TrimSpace(sql)
	if sql == "" {
		return "", ""
	}
	end := 0
	for end < len(sql) && isSQLIdent(sql[end]) {
		end++
	}
	if end == 0 {
		return "", ""
	}
	return strings.ToUpper(sql[:end]), sql[end:]
}

func hasClause(stm *gorm.Statement, name string) bool {
	_, ok := stm.Clauses[name]
	return ok
}

func hasNoCache(stm *gorm.Statement) bool {
	if _, ok := stm.Clauses[NoCache]; ok {
		return true
	}
	return hasNoCacheSetting(stm)
}

func hasNoCacheSetting(stm *gorm.Statement) bool {
	if _, ok := stm.Get(NoCache); ok {
		return true
	}
	if _, ok := stm.InstanceGet(NoCache); ok {
		return true
	}
	return false
}

func hasQueryModifiers(stm *gorm.Statement) bool {
	return stm.DB != nil && stm.DryRun ||
		len(stm.Selects) > 0 ||
		len(stm.Omits) > 0 ||
		len(stm.Joins) > 0 ||
		len(stm.Preloads) > 0 ||
		hasClause(stm, "FOR")
}

func indexCTEBody(sql string) int {
	upper := strings.ToUpper(sql)
	for i := 0; i+2 <= len(upper); i++ {
		if !strings.HasPrefix(upper[i:], "AS") {
			continue
		}
		if i > 0 && isSQLIdent(upper[i-1]) {
			continue
		}
		j := i + 2
		if j < len(upper) && isSQLIdent(upper[j]) {
			continue
		}
		for j < len(sql) && (sql[j] == ' ' || sql[j] == '\t' || sql[j] == '\n' || sql[j] == '\r') {
			j++
		}
		if j < len(sql) && sql[j] == '(' {
			return j
		}
	}
	return -1
}

func inTransaction(stm *gorm.Statement) bool {
	if stm == nil || stm.ConnPool == nil {
		return false
	}
	_, ok := stm.ConnPool.(gorm.TxCommitter)
	return ok
}

func isMutatingSQL(sql string) bool {
	sql = stripLeadingSQLComments(strings.TrimSpace(sql))
	first, rest := firstSQLToken(sql)
	if first == "" {
		return false
	}
	if first == "WITH" {
		final, cteMutates := cteFinalToken(rest)
		if cteMutates {
			return true
		}
		first = final
	}
	return isMutatingSQLVerb(first)
}

func isMutatingSQLVerb(verb string) bool {
	switch verb {
	case "WITH":
		return true
	case "INSERT", "UPDATE", "DELETE", "REPLACE", "UPSERT", "MERGE", "ALTER", "DROP", "TRUNCATE", "CREATE", "CALL", "EXEC", "EXECUTE":
		return true
	default:
		return false
	}
}

func isSingleStructDest(dest any) bool {
	if dest == nil {
		return false
	}
	typ := reflect.TypeOf(dest)
	if typ.Kind() != reflect.Pointer {
		return false
	}
	elem := typ.Elem()
	if elem.Kind() == reflect.Pointer {
		elem = elem.Elem()
	}
	return elem.Kind() == reflect.Struct
}

func isSQLIdent(ch byte) bool {
	return ch >= 'A' && ch <= 'Z' || ch >= 'a' && ch <= 'z' || ch >= '0' && ch <= '9' || ch == '_'
}

func matchingParen(sql string) int {
	depth := 0
	for i := 0; i < len(sql); i++ {
		switch sql[i] {
		case '(':
			depth++
		case ')':
			depth--
			if depth == 0 {
				return i
			}
		}
	}
	return -1
}

func primaryValue(stm *gorm.Statement) string {
	if stm == nil || stm.Schema == nil || len(stm.Schema.PrimaryFields) != 1 {
		return ""
	}
	field := stm.Schema.PrimaryFields[0]
	value := stm.ReflectValue
	for value.Kind() == reflect.Pointer {
		if value.IsNil() {
			return ""
		}
		value = value.Elem()
	}
	if value.Kind() != reflect.Struct {
		return ""
	}
	val, zero := field.ValueOf(context.Background(), value)
	if zero {
		return ""
	}
	return toString(val)
}

func skipAfterSQLBuild(stm *gorm.Statement) bool {
	if primaryFromWhere(stm) != "" {
		return false
	}
	limitClause, ok := stm.Clauses["LIMIT"]
	if !ok {
		return true
	}
	limit, ok := limitClause.Expression.(clause.Limit)
	return !ok || limit.Limit == nil || *limit.Limit != 1
}

func skipInvalidateStatement(stm *gorm.Statement) bool {
	if stm == nil || stm.Schema == nil || stm.Table == "" {
		return true
	}
	return false
}

func skipStatement(stm *gorm.Statement) bool {
	if stm == nil || stm.Schema == nil || stm.Table == "" {
		return true
	}
	if len(stm.Schema.PrimaryFields) != 1 {
		return true
	}
	if inTransaction(stm) {
		return true
	}
	if hasQueryModifiers(stm) {
		return true
	}
	if hasNoCache(stm) {
		return true
	}
	return !isSingleStructDest(stm.Dest)
}

func stripLeadingSQLComments(sql string) string {
	for {
		sql = strings.TrimSpace(sql)
		switch {
		case strings.HasPrefix(sql, "--"):
			if idx := strings.IndexByte(sql, '\n'); idx >= 0 {
				sql = sql[idx+1:]
				continue
			}
			return ""
		case strings.HasPrefix(sql, "/*"):
			if idx := strings.Index(sql, "*/"); idx >= 0 {
				sql = sql[idx+2:]
				continue
			}
			return ""
		default:
			return sql
		}
	}
}

func toString(val any) string {
	switch typed := val.(type) {
	case string:
		return typed
	case []byte:
		return string(typed)
	default:
		return fmt.Sprint(val)
	}
}
