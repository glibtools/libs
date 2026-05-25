package gormcache

import (
	"crypto/rand"
	"encoding/hex"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

type rowToken struct {
	table  string
	pk     string
	global string
	sweep  string
	epoch  string
}

type tableEpochKeys struct {
	table string
	sweep string
}

type tableEpochs struct {
	prefix    string
	store     Store
	globalKey string
	tableKeys sync.Map
	initMu    sync.Mutex
	fallback  atomic.Uint64
}

func (e *tableEpochs) bumpGlobal() {
	if e == nil {
		return
	}
	e.bumpKey(e.globalKey)
}

func (e *tableEpochs) bumpKey(key string) {
	if e == nil || e.store == nil {
		return
	}
	e.store.Set(key, []byte(e.next()), 0)
}

func (e *tableEpochs) bumpRow(table, pk string) {
	if e == nil || table == "" || pk == "" {
		return
	}
	keys := e.keysForTable(table)
	e.bumpKey(keys.table)
	e.bumpKey(e.key("row", table, pk))
}

func (e *tableEpochs) bumpTable(table string) {
	if e == nil || table == "" {
		return
	}
	keys := e.keysForTable(table)
	e.bumpKey(keys.table)
	e.bumpKey(keys.sweep)
}

func (e *tableEpochs) key(parts ...string) string {
	items := make([]string, 0, len(parts)+2)
	items = append(items, e.prefix, "epoch")
	items = append(items, parts...)
	return strings.Join(items, keySep)
}

func (e *tableEpochs) keysForTable(table string) tableEpochKeys {
	if got, ok := e.tableKeys.Load(table); ok {
		return got.(tableEpochKeys)
	}
	keys := tableEpochKeys{
		table: e.key("table", table),
		sweep: e.key("sweep", table),
	}
	got, _ := e.tableKeys.LoadOrStore(table, keys)
	return got.(tableEpochKeys)
}

func (e *tableEpochs) load(parts ...string) string {
	if e == nil {
		return e.next()
	}
	return e.loadKey(e.key(parts...))
}

func (e *tableEpochs) loadGlobal() string {
	if e == nil {
		return e.next()
	}
	return e.loadKey(e.globalKey)
}

func (e *tableEpochs) loadKey(key string) string {
	if e == nil || e.store == nil {
		return e.next()
	}
	val, ok := e.store.Get(key)
	if ok && len(val) > 0 {
		return string(val)
	}

	token := e.next()
	e.initMu.Lock()
	defer e.initMu.Unlock()
	val, ok = e.store.Get(key)
	if ok && len(val) > 0 {
		return string(val)
	}
	e.store.Set(key, []byte(token), 0)
	return token
}

func (e *tableEpochs) loadRow(table, pk string) string {
	return e.loadKey(e.key("row", table, pk))
}

func (e *tableEpochs) loadSweep(table string) string {
	return e.loadKey(e.keysForTable(table).sweep)
}

func (e *tableEpochs) loadTable(table string) string {
	return e.loadKey(e.keysForTable(table).table)
}

// next returns a 64-bit random token. A collision would require the same epoch
// scope to bump into an old token; birthday risk only becomes material around
// 2^32 bumps in that single scope, which is negligible for this invalidation use.
func (e *tableEpochs) next() string {
	var buf [8]byte
	if _, err := rand.Read(buf[:]); err == nil {
		return hex.EncodeToString(buf[:])
	}
	if e == nil {
		return strconv.FormatInt(time.Now().UnixNano(), 36)
	}
	return strconv.FormatInt(time.Now().UnixNano(), 36) + "-" + strconv.FormatUint(e.fallback.Add(1), 36)
}

func (e *tableEpochs) snapshotRow(table, pk, global string) rowToken {
	return rowToken{
		table:  table,
		pk:     pk,
		global: global,
		sweep:  e.loadSweep(table),
		epoch:  e.loadRow(table, pk),
	}
}

func (e *tableEpochs) snapshotTable(table string) tableToken {
	return tableToken{table: table, global: e.loadGlobal(), epoch: e.loadTable(table)}
}

func (e *tableEpochs) validRow(token rowToken) bool {
	return e.loadGlobal() == token.global &&
		e.loadSweep(token.table) == token.sweep &&
		e.loadRow(token.table, token.pk) == token.epoch
}

func (e *tableEpochs) validTable(token tableToken) bool {
	return e.loadGlobal() == token.global &&
		e.loadTable(token.table) == token.epoch
}

type tableToken struct {
	table  string
	global string
	epoch  string
}

func newTableEpochs(prefix string, store Store) *tableEpochs {
	e := &tableEpochs{prefix: prefix, store: store}
	e.globalKey = e.key("global")
	return e
}
