# gormcache 工具化契约（Phase 4）

> `pkg/mdb/gormcache` 是 GORM 单行查询缓存插件。这份文档定义支持范围、不支持范围、TTL / key / 降级策略、失败模式、定向验证清单。
> 与 `go/pkg/mdb/gormcache/doc.go`、`plugin.go`、`store.go` 共同构成契约：代码里的 godoc 是行内说明，本文件是「跨项目接入也要遵守的合同」。

## 1. 定位

- GORM 查询缓存插件。**仅支持「单主键、单行」查询**。
- 走 GORM 的 callbacks：Query / Create / Update / Delete / Raw。
- 缓存值 + 表 / 行 / 全局 invalidation epoch 双层 store。
- 默认 backend 是 `freecache`（in-process LRU + 容量上限）。

适合：
- 高频按主键读取、低频写入的小表。
- 业务可接受短期最终一致（TTL ≤ 数分钟）。

不适合：
- 多条件 / range / join / count / 聚合查询 → 不走缓存，会 fallthrough 到 GORM。
- 多主键复合 PK → 同上。
- 必须强一致（金额 / 库存 / 防超卖）→ 不要用。

## 2. 公开 API

```go
type Config struct {
    Prefix             string
    Disabled           bool
    Store              Store
    StoreSize          int
    EpochStore         Store
    EpochStoreSize     int
    TTLSeconds         int
    NegativeTTLSeconds int
    Marshal            func(any) ([]byte, error)
    Unmarshal          func([]byte, any) error
}

type Store interface {
    Get(key string) ([]byte, bool)
    Set(key string, val []byte, ttlSeconds ...int)
    Del(key string)
    DropPrefix(prefix ...string)
}

type Stats struct {
    Hits, Misses, Invalidations uint64
    StoreSetErrors, StoreEvictions uint64
}

func New(cfg Config) *Plugin
func Install(db *gorm.DB, cfg Config) (*Plugin, error)

func (p *Plugin) Enable()
func (p *Plugin) Disable()
func (p *Plugin) Enabled() bool
func (p *Plugin) Stats() Stats
func (p *Plugin) InvalidateAll()
func (p *Plugin) InvalidateTable(table string)
func (p *Plugin) InvalidateRow(table, pkValue string)

func NewFreeCacheStore(size int) *FreeCacheStore
func NewNamespaceStore(inner Store, namespaceFor func(key string) string) *NamespaceStore

const NoCache = "gormcache:no-cache"
var NoCacheClause clause.Expression
```

## 3. 默认参数

| key | 默认值 | 含义 |
|---|---|---|
| `Prefix` | `gormcache` | 内部 key 前缀，可隔离多 plugin 实例 |
| `TTLSeconds` | `600` | 正缓存 TTL（秒） |
| `NegativeTTLSeconds` | `60` | 「未找到」缓存 TTL（秒），用于挡住 stamp 风暴 |
| `StoreSize` | `64<<20` (64MB) | 值缓存容量；`freecache` 单条目 ≤ 容量/1024 |
| `EpochStoreSize` | 由 `StoreSize` 推导（见 `derivedEpochStoreSize`） | epoch 缓存容量 |
| `Marshal` | `encoding/json.Marshal` | 缓存值编码 |
| `Unmarshal` | `encoding/json.Unmarshal` | 缓存值解码 |
| `Disabled` | `false` | 启用即 enabled |
| `Store` | `NewFreeCacheStore(StoreSize)` | 值 store；可替换为其他 backend |
| `EpochStore` | 独立 FreeCache | epoch store；共享时多实例可联动 invalidation |

**所有 TTL 字段都是 `int` 秒**（历史上有过别的单位，已统一）。0 / 负数走默认；想要禁用 TTL 需要替换 store。

## 4. 命中 / 未命中流程

```
GORM SELECT
  ├ skipStatement?           → fallthrough 到原始 gorm:query
  ├ buildQuerySQL
  ├ skipAfterSQLBuild?       → fallthrough
  ├ tryCache(key)
  │   ├ HIT                  → 写入 Dest，++Hits，return
  │   └ MISS                 → ++Misses
  ├ singleflight.Do(key)
  │   ├ leader: 真正跑 SELECT，fillCache(key)，return flightResult
  │   └ shared follower: 复用 leader 结果；若 shareable=true 直接写 Dest，否则 retry
  └ retrySharedMiss(fresh token)  ← 让 follower 在 leader 失败时再走一次
```

key 生成（见 `keys.go`）：
- 主键查询 `WHERE pk = ?` / GORM `First(&dst, id)`：构造 `primaryKey`（按 row epoch）。
- 其他 single-row：构造 `searchKey`（按 table epoch + digest），缓存 pk 值用于回指 primary key。

## 5. 失效（invalidation）

写操作 → `invalidate` callback：
- 能从 `Statement` 拿到 pk → `InvalidateRow(table, pk)`（精准）。
- 不能拿 pk（例如 `UPDATE foo SET x=? WHERE y=?`）→ `InvalidateTable(table)`（粗粒度）。
- Raw SQL 而且是 mutating（INSERT/UPDATE/DELETE/REPLACE/CREATE/DROP/TRUNCATE/ALTER）→ `InvalidateAll`。

invalidation 在 `Disabled` 期间仍然运行：避免「关 → 写 → 开」窗口期暴露脏缓存。

API 触发：
- `InvalidateAll()` → bump global epoch。
- `InvalidateTable(t)` → bump table epoch。
- `InvalidateRow(t, pk)` → bump row epoch 并 `Del(primaryKey)`。

## 6. 一致性失败模式（必须知道）

1. **窗口期 stale**：缓存 SET 与 epoch bump 不在同一原子操作。写后 `TTLSeconds` 内仍可能读到旧值（行级精度），最差为 TTL 上限。
2. **Search key 暴露漏洞**：search key 缓存的是「pk 字符串」，靠 row epoch 回指 primary key 校验。如果 epoch store 写丢（容量驱逐）→ 可能短暂回指失败，表现为 miss + 重读，不会读到旧值。
3. **freecache 容量驱逐**：`StoreSize` 太小时写不进 + LRU 驱逐高频访问，会出现 `StoreSetErrors` / `StoreEvictions` 上涨。把 metric 暴露出来。
4. **大记录写入失败**：freecache 拒绝大于 `StoreSize / 1024` 的条目。命中 `SetErrors` 增长。
5. **多实例不共享**：默认 in-process。多容器实例各自维护一份缓存 + 各自 epoch；写在 A 不会让 B 失效。多副本场景必须共享 EpochStore（例如换 Redis 实现）或 TTL 内接受不一致。
6. **JSON marshal 不一致**：默认 `encoding/json`，类型必须能稳定 marshal/unmarshal。自定义 `MarshalJSON` 漂移会导致缓存值与 GORM `Find(&dst)` 不一致。

## 7. 跳过 / 降级

- 进程中临时跳过：在 `*gorm.DB` 上挂 `Set(gormcache.NoCache, true)` 或 `Clauses(gormcache.NoCacheClause)`。
- 永久关闭：`Disabled=true` 构造或运行时 `Plugin.Disable()`。invalidation 仍然生效。
- 任何 `db.Error != nil` 路径不缓存。`gorm.ErrRecordNotFound` 走负缓存（`NegativeTTLSeconds`）。
- Plugin 不影响事务：默认 invalidate 在 `commit_or_rollback_transaction` 之后；事务 rollback 不会触发 invalidate（已 join 的 `db.Error`）。

## 8. 在 prize_draw 内的状态

- 业务侧调用通过 `Install` 注入 fx graph；具体使用点见 `core/` 各 repository。
- 已统一 TTL 单位为「秒（int）」，包含 `Config.TTLSeconds` 和 `Config.NegativeTTLSeconds`（见 IDEAS.md 已完成项）。
- `gormcache_test.go` 在 `.gitignore` 全局例外里**已 `git add -f` 入库**，按已跟踪文件正常 commit，是当前事实测试套件。

## 9. 定向验证（必须能跑）

下面命令对应 `Plugin` 的核心契约。每条都来自 `go/pkg/mdb/gormcache/gormcache_test.go` 的真实用例：

```bash
cd go
go test ./pkg/mdb/gormcache -count=1
go test ./pkg/mdb/gormcache -run TestPluginHitMiss -count=1
go test ./pkg/mdb/gormcache -run TestPluginNegativeTTL -count=1
go test ./pkg/mdb/gormcache -run TestPluginInvalidation -count=1
```

不能跑（CI 没有 DB / SQLite 缺依赖）时，至少跑 `go vet ./pkg/mdb/gormcache`。

定向场景：
- **HIT / MISS**：先 First(&dst, id) → 期望 Misses+1；第二次 → Hits+1。
- **Negative cache**：First(&dst, "missing") → 返回 ErrRecordNotFound，第二次 → 不打 DB（profile via SQL log）。
- **Row invalidation**：Update by pk → 立刻 First(&dst, id) 期望命中新值。
- **Table invalidation**：Update where 非 pk → InvalidateTable → 同表其他 row 第一次必为 miss。
- **Raw mutating SQL**：`db.Exec("DELETE FROM ... WHERE non_pk = ?")` → InvalidateAll。
- **Disabled / NoCache**：Disable 后 Hits 不再增长；invalidation 仍然增加 epoch（写时仍发生 bump）。

## 10. 依赖与传递依赖

- 直接依赖：`gorm.io/gorm`、`golang.org/x/sync/singleflight`、`github.com/coocood/freecache`。
- 传递依赖：`freecache` 间接拉 `github.com/cespare/xxhash/v2`；**`xxhash` 不是业务直接依赖**，不应在 prize_draw 业务包里 import。

迁移到独立 module（Phase 8）时：
- `freecache` 要继续作为 default store。`Store` 接口允许调用方换 Redis / BadgerDB。
- 不要把 `xxhash` 升到 direct require；保持「传递依赖」。

## 11. 虚拟第二消费者接入清单

> 新项目 `acme-orders` 想给「按主键查 User 表」加缓存。

接入步骤：

1. import：`github.com/flyltgg/prize_draw/pkg/mdb/gormcache`。
2. 装配：
   ```go
   plugin, err := gormcache.Install(db, gormcache.Config{
       Prefix:             "acme-orders",
       StoreSize:          32 << 20,
       TTLSeconds:         300,
       NegativeTTLSeconds: 30,
   })
   if err != nil { /* fatal */ }
   ```
3. 用法：照旧 `db.First(&user, id)` / `db.Updates(&user)`；不需要写新 API。
4. 跳过：`db.Set(gormcache.NoCache, true).First(&user, id)`。
5. 监控：定时拉 `plugin.Stats()`，曝光 hit ratio / SetErrors / Evictions。
6. 验证：在 `acme-orders` 自己的 test 里复现 §9 的 6 个定向场景。

接入清单失败例：
- 想缓存 `List` 查询 → 不支持，gormcache 只缓存 single-row。
- 想用 Redis 当 backend → 实现 `Store` 接口（Get/Set/Del/DropPrefix），传入 `Config.Store`。
- 想要「缓存预热」→ 自己启动时跑 `db.First(&dst, id)`；plugin 不提供 warmup。
- 想要「按业务 namespace 分桶」→ 用 `NewNamespaceStore` 包一层。

## 12. 已知 trade-off

- `singleflight` 只在 leader 失败 + shared follower 时重投一次（`retrySharedMiss`）；理论上仍可能有「follower 拿到 unshareable 结果」的情形，会触发回源。
- Negative TTL 默认 60s 比 positive 短，避免「记错的 not-found」长期挡掉真正写入的记录。
- `NoCache` 既是 GORM `Set` 的 key，也是 `clause.Expression`，两种用法等价。

## 13. 反向边界（不要做）

- 不要在 plugin 内加 metrics / log 包依赖。Stats 是 pull 模型，调用方自己接监控。
- 不要支持「写穿」缓存（write-through）；这会复活脏写问题。
- 不要把 callbacks 顺序变成可调；当前顺序（query replace + after commit）是行为契约的一部分。
- 不要把 `Marshal`/`Unmarshal` 当作可热切换；构造后切换不安全。
