package mdb

import (
	"crypto/md5"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"log"
	"reflect"
	"regexp"
	"strings"
	"sync"
	"time"

	"github.com/spf13/cast"
	"gorm.io/driver/mysql"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
	"gorm.io/gorm/schema"

	"github.com/glibtools/libs/util"
)

var (
	DB                       = NewGormDB()
	ErrRecordExists          = errors.New("record exists")
	defaultDatetimePrecision = 9
	globalDBModels           = make([]interface{}, 0)
	globalModelBus           interface{}
	localSyncMap             = &sync.Map{}
)

type DBOption struct {
	Type string `json:"type"`
	Host string `json:"host"`
	Port string `json:"port"`
	User string `json:"user"`
	Pwd  string `json:"pwd"`
	DB   string `json:"db"`

	SkipCache bool   `json:"skip_cache"`
	CacheType string `json:"cache_type"`

	Logger logger.Interface `json:"-"`

	MaxIdleConns       int   `json:"max_idle_conns"`
	MaxOpenConns       int   `json:"max_open_conns"`
	MaxLifetimeSeconds int64 `json:"max_lifetime_seconds"`
	MaxIdleTimeSeconds int64 `json:"max_idle_time_seconds"`

	SkipCreateDB bool `json:"skipCreateDb,omitempty"`
}

// DSN ...
func (d *DBOption) DSN() string { return d.parseDSN() }

// DSNMd5 ...
func (d *DBOption) DSNMd5() string { return Md5([]byte(d.DSN())) }

// GetLogger ...
func (d *DBOption) GetLogger() logger.Interface {
	if d.Logger == nil {
		d.Logger = NewDBLoggerWithLevel(logger.Info)
	}
	return d.Logger
}

func (d *DBOption) dbInitiate() (db *gorm.DB, err error) {
	dsn := d.parseDSN()
	var dialectal gorm.Dialector
	switch d.Type {
	case "mysql":
		dialectal = mysql.New(mysql.Config{
			DriverName:               "mysql",
			DSN:                      dsn,
			DefaultStringSize:        256,
			DefaultDatetimePrecision: &defaultDatetimePrecision,
			DisableDatetimePrecision: true,
			DontSupportRenameIndex:   true,
		})
	case "pg", "postgres":
		dialectal = postgres.New(postgres.Config{DriverName: "pgx", DSN: dsn})
	default:
		panic("unknown db type")
	}
	gormConfig := &gorm.Config{Logger: d.GetLogger()}
	db, err = gorm.Open(dialectal, gormConfig)
	if err != nil {
		return
	}
	sqlDB, err := db.DB()
	if err != nil {
		return
	}
	// SetMaxOpenConns 设置打开数据库连接的最大数量
	if d.MaxOpenConns <= 0 {
		d.MaxOpenConns = 200
	}
	if d.MaxOpenConns >= 2000 {
		d.MaxOpenConns = 2000
	}
	//sqlDB.SetMaxOpenConns(200)
	sqlDB.SetMaxOpenConns(d.MaxOpenConns)

	if d.MaxIdleConns <= 0 {
		d.MaxIdleConns = 50
	}
	if d.MaxIdleConns >= d.MaxOpenConns {
		d.MaxIdleConns = d.MaxOpenConns / 2
	}
	// SetMaxIdleConns 设置空闲连接池中连接的最大数量
	//sqlDB.SetMaxIdleConns(50)
	sqlDB.SetMaxIdleConns(d.MaxIdleConns)

	if d.MaxLifetimeSeconds <= 0 {
		d.MaxLifetimeSeconds = 30
	}
	if d.MaxLifetimeSeconds >= 3600 {
		d.MaxLifetimeSeconds = 3600
	}
	// SetConnMaxLifetime 设置了连接可复用的最大时间
	//sqlDB.SetConnMaxLifetime(time.Hour * 10)
	sqlDB.SetConnMaxLifetime(time.Duration(d.MaxLifetimeSeconds) * time.Second)
	//sqlDB.SetConnMaxLifetime(time.Second * 59)

	if d.MaxIdleTimeSeconds <= 0 {
		d.MaxIdleTimeSeconds = 30
	}
	if d.MaxIdleTimeSeconds >= d.MaxLifetimeSeconds {
		d.MaxIdleTimeSeconds = d.MaxLifetimeSeconds * 80 / 100
	}
	// SetConnMaxIdleTime 设置空闲连接池中连接的最大空闲时间
	//sqlDB.SetConnMaxIdleTime(time.Hour * 11)
	sqlDB.SetConnMaxIdleTime(time.Duration(d.MaxIdleTimeSeconds) * time.Second)
	//sqlDB.SetConnMaxIdleTime(time.Second * 60)
	if err = sqlDB.Ping(); err != nil {
		return
	}
	//go util.Ticker(time.Second*30, func() {
	//	if e := sqlDB.Ping(); e != nil {
	//		d.GetLogger().Error(context.Background(), "db ping error: %s", e.Error())
	//	}
	//})
	//d.GetLogger().Info(context.Background(), "db connected")

	if !d.SkipCache {
		gm2opt := Config{
			Skip:   d.SkipCache,
			TTL:    60 * 10,
			Prefix: d.DSNMd5(),
			Store:  d.getCacheStore(),
		}
		if err = db.Use(NewPlugin(gm2opt)); err != nil {
			return
		}
	}

	log.Printf("db %s connected\n", d.DB)
	return
}

// getCacheStore ...
func (d *DBOption) getCacheStore() CacheStore {
	switch d.CacheType {
	case "redis":
		return GetRedisStore(Rdb.GetClient(1))
	case "cc":
		return GetCCacheStore()
	default:
		return GetFreeCacheStore()
	}
}

// parseDSN ...
func (d *DBOption) parseDSN() string {
	switch d.Type {
	case "mysql":
		dsn := `{{.user}}:{{.pwd}}@tcp({{.host}}:{{.port}})/{{.db}}?charset=utf8mb4&collation=utf8mb4_bin&timeout=5s&loc=Local&parseTime=True`
		dsn = util.TextTemplateMustParse(dsn, d)
		return dsn
	case "pg", "postgres":
		dsn := `host={{.host}}
user={{.user}}
password={{.pwd}}
dbname={{.db}}
port={{.port}}
sslmode=disable
TimeZone=Asia/Shanghai`
		dsn = util.TextTemplateMustParse(dsn, d)
		return dsn
	default:
		panic("unknown db type")
	}
}

type GormDB struct {
	*gorm.DB
	opt *DBOption

	models map[string]interface{}

	viewModels map[string]interface{}
}

func (g *GormDB) CheckDBNil() (err error) {
	if g.DB == nil {
		return errors.New("database is nil")
	}
	return
}

// Close ...
func (g *GormDB) Close() (err error) {
	// check nil
	if err = g.CheckDBNil(); err != nil {
		return
	}
	sqlDB, err := g.DB.DB()
	if err != nil {
		return
	}
	return sqlDB.Close()
}

func (g *GormDB) CreateDB() {
	if err := g.createDB(); err != nil {
		log.Fatalln(err)
	}
}

// DropDB ...
func (g *GormDB) DropDB() {
	if err := g.dropDB(); err != nil {
		log.Fatalln(err)
	}
}

// GetFindModel ...
func (g *GormDB) GetFindModel(table string) (interface{}, error) {
	if table == "" {
		return nil, errors.New("the table is empty")
	}
	m := mergeModels(g.models, g.viewModels)
	if v, ok := m[table]; ok {
		return util.NewValue(v), nil
	}
	return nil, errors.New("table's model isn't found")
}

// GetModel ...
func (g *GormDB) GetModel(table string) (interface{}, error) {
	if table == "" {
		return nil, errors.New("the table is empty")
	}
	if v, ok := g.models[table]; ok {
		return util.NewValue(v), nil
	}
	return nil, errors.New("table's model isn't found")
}

// Initialize GormDB
func (g *GormDB) Initialize(opt *DBOption) *GormDB {
	g.opt = opt
	var e error

	if !opt.SkipCreateDB {
		if e = g.createDB(); e != nil {
			log.Fatalf("GormDB create database error: %s", e.Error())
		}
	}

	g.DB, e = g.opt.dbInitiate()
	if e != nil {
		log.Fatalf("GormDB initialize error: %s", e.Error())
	}
	return g
}

// MigrateModels ...
func (g *GormDB) MigrateModels(v ...interface{}) {
	migrate := false
	models := make([]interface{}, 0)
	for _, vi := range v {
		if vi == nil {
			continue
		}
		if b, ok := vi.(bool); ok {
			migrate = b
			continue
		}
		models = append(models, vi)
	}
	models = append(models, globalDBModels...)
	for _, vi := range models {
		g.RegModel(vi)
	}
	if globalModelBus != nil {
		g.RegModelBus(globalModelBus)
	}
	if len(g.models) == 0 {
		return
	}

	if hasMigrateLockFile() || !migrate {
		return
	}

	models = models[:0]
	for _, model := range g.models {
		models = append(models, model)
	}

	e := g.AutoMigrate(models...)
	if e != nil {
		log.Fatalf("GormDB AutoMigrate models error: %s", e.Error())
	}
	for _, model := range models {
		if m, ok := model.(ItfModelInitializer); ok {
			_e := g.initializeModel(m)
			if _e != nil {
				log.Fatalf("ItfModelInitializer data error: %s", _e.Error())
			}
		}
	}
	createMigrateLockFile()
	log.Println("GormDB AutoMigrate models success")
}

// ModelByTableName ...
func (g *GormDB) ModelByTableName(tableName string) (model interface{}, err error) {
	if err = g.CheckDBNil(); err != nil {
		return
	}
	m, ok := g.models[tableName]
	if !ok {
		return nil, fmt.Errorf("model %s not found", tableName)
	}
	t := reflect.TypeOf(m)
	for t.Kind() == reflect.Ptr {
		t = t.Elem()
	}
	model = reflect.New(t).Interface()
	return
}

// ModelTableName ...
func (g *GormDB) ModelTableName(model interface{}) (name string) {
	if err := g.CheckDBNil(); err != nil {
		panic(err)
	}
	return ModelTableName(model)
}

func (g *GormDB) Models() map[string]interface{} { return g.models }

func (g *GormDB) Opt() *DBOption { return g.opt }

// RegModel ...
func (g *GormDB) RegModel(model interface{}) {
	tableName := g.ModelTableName(model)
	if _, ok := g.models[tableName]; !ok {
		g.models[tableName] = model
	}
}

// RegModelBus ...
func (g *GormDB) RegModelBus(bus interface{}) {
	models := util.ObjectTagInstances(bus, "model")
	for _, model := range models {
		g.RegModel(model)
	}
}

// RegViewModel ...
func (g *GormDB) RegViewModel(model interface{}) {
	tableName := g.ModelTableName(model)
	if _, ok := g.viewModels[tableName]; !ok {
		g.viewModels[tableName] = model
	}
}

func (g *GormDB) SetOpt(opt *DBOption) { g.opt = opt }

func (g *GormDB) ViewModels() map[string]interface{} { return g.viewModels }

// createDB create database
func (g *GormDB) createDB() (err error) {
	db, err := g.openDefaultDB()
	// create database
	if err != nil {
		return
	}
	sdb, err := db.DB()
	if err != nil {
		return
	}
	defer func() { _ = sdb.Close() }()

	switch g.opt.Type {
	case "mysql":
		err = db.Exec(fmt.Sprintf("CREATE DATABASE IF NOT EXISTS %s;", g.opt.DB)).Error
	case "pg", "postgres":
		var cc int64
		err = db.Raw("SELECT count(*) FROM pg_database WHERE datname = ?;", g.opt.DB).Count(&cc).Error
		if err != nil {
			return
		}
		if cc > 0 {
			return
		}
		if err = db.Exec(fmt.Sprintf("CREATE DATABASE %s;", g.opt.DB)).Error; err != nil {
			return
		}
	}
	return
}

func (g *GormDB) dropDB() (err error) {
	db, err := g.openDefaultDB()
	// create database
	if err != nil {
		return
	}
	sdb, err := db.DB()
	if err != nil {
		return
	}
	defer func() { _ = sdb.Close() }()
	err = db.Exec(fmt.Sprintf("DROP DATABASE IF EXISTS %s;", g.opt.DB)).Error
	return
}

// initializeModel ...
func (g *GormDB) initializeModel(model ItfModelInitializer) (err error) {
	tbName := g.ModelTableName(model)
	beans, err := model.InitData(g.DB)
	if err != nil {
		return
	}
	if beans == nil {
		return
	}
	var count int64
	if err = g.Table(tbName).Count(&count).Error; err != nil {
		return
	}
	if count > 0 {
		return
	}
	//switch g.opt.Type {
	//case "mysql":
	//	if err = g.Exec(fmt.Sprintf("TRUNCATE TABLE %s;", tbName)).Error; err != nil {
	//		return
	//	}
	//case "pg", "postgres":
	//	if err = g.Exec(fmt.Sprintf("TRUNCATE TABLE %s RESTART IDENTITY;", tbName)).Error; err != nil {
	//		return
	//	}
	//}
	if err = g.Table(tbName).CreateInBatches(beans, 100).Error; err != nil {
		return
	}
	return
}

// openDefaultDB open database
func (g *GormDB) openDefaultDB() (db *gorm.DB, err error) {
	dsn := g.opt.parseDSN()
	switch g.opt.Type {
	case "mysql":
		dsn = regexp.MustCompile(`/\w+\?`).ReplaceAllString(dsn, "/mysql?")
		db, err = gorm.Open(mysql.Open(dsn))
	case "pg", "postgres":
		dsn = regexp.MustCompile(`dbname=\w+`).ReplaceAllString(dsn, "dbname=postgres")
		db, err = gorm.Open(postgres.Open(dsn))
	default:
		err = fmt.Errorf("unknown database type: %s", g.opt.Type)
	}
	return
}

type ItfModelInitializer interface {
	InitData(db *gorm.DB) (interface{}, error)
}

type argsTagModel struct {
	// default false
	AutoDelete bool `json:"auto_delete,omitempty"`
	// default days:90
	// days: save days
	// count: save rows count
	Save string `json:"save,omitempty"`
	Val  int    `json:"val,omitempty"`
}

func (a *argsTagModel) delete(db *gorm.DB, model interface{}) {
	if !a.AutoDelete {
		return
	}
	switch a.Save {
	case "days":
		a.deleteByDays(db, model, a.Val)
	case "count":
		a.deleteByCount(db, model, a.Val)
	}
}

func (*argsTagModel) deleteByCount(db *gorm.DB, model interface{}, saveCount int) {
	if saveCount <= 0 {
		return
	}
	sq1 := `id <= (SELECT id FROM {{.table}} ORDER BY id DESC LIMIT 1 OFFSET {{.save_count}});`
	sq1 = util.TextTemplateMustParse(sq1, util.Map{
		"table":      ModelTableName(model),
		"save_count": saveCount,
	})
	var count int64
	db.Model(model).Where(sq1).Count(&count)
	if count <= 0 {
		return
	}
	BatchDeleteFromTop(db, model, int(count))
}

func (*argsTagModel) deleteByDays(db *gorm.DB, model interface{}, days int) {
	if days <= 0 {
		return
	}
	days--
	const layout = "2006-01-02"
	str := time.Now().AddDate(0, 0, -days).Format(layout)
	time1, _ := time.ParseInLocation(layout, str, time.Local)
	var count int64
	db.Model(model).Where("created_at < ?", time1).Count(&count)
	if count <= 0 {
		return
	}
	BatchDeleteFromTop(db, model, int(count))
}

func AddModels(v ...interface{}) {
	globalDBModels = append(globalDBModels, v...)
}

func AutoDelete(db *gorm.DB, bus interface{}) {
	if db == nil || bus == nil {
		return
	}
	val := util.ReflectIndirect(bus)
	for i := 0; i < val.NumField(); i++ {
		fieldType := val.Type().Field(i)
		modelTags := modelTagParse(fieldType.Tag.Get("model"))
		if modelTags == nil {
			continue
		}
		newFieldValue := reflect.New(fieldType.Type.Elem())
		modelTags.delete(db, newFieldValue.Interface())
	}
}

// BatchDeleteFromTop eachCount = 1000;
// count: count of delete rows , order by id asc
func BatchDeleteFromTop(db *gorm.DB, model interface{}, count int) {
	if count <= 0 {
		return
	}
	const eachCount = 1000
	var loopCount = count / eachCount
	var remain = count % eachCount
	if remain > 0 {
		loopCount++
	}
	for i := 1; i <= loopCount; i++ {
		var currentMaxID uint64
		tx1 := db.Model(model).Select("id").Order("id").Limit(1)
		func() *gorm.DB {
			if remain > 0 && i == loopCount {
				return tx1.Offset(remain)
			}
			return tx1.Offset(eachCount)
		}()
		tx1.Scan(&currentMaxID)
		if currentMaxID <= 0 {
			break
		}
		db.Where("id < ?", currentMaxID).Delete(model)
	}
}

func CreateIfNotExists(db *gorm.DB, bean interface{}, query interface{}, args ...interface{}) error {
	var count int64
	db.Model(util.NewValue(bean)).Where(query, args...).Count(&count)
	if count > 0 {
		return ErrRecordExists
	}
	return db.Create(bean).Error
}

// Md5 returns the MD5 checksum string of the data.
func Md5(b []byte) string {
	checksum := md5.Sum(b)
	return hex.EncodeToString(checksum[:])
}

func ModelTableName(model interface{}) string {
	s, err := ParseModel(model)
	if err != nil {
		panic(err)
	}
	return s.Table
}

func NewDBLoggerSilent() logger.Interface {
	return logger.New(
		log.New(io.Discard, "", log.LstdFlags),
		logger.Config{LogLevel: logger.Silent},
	)
}

func NewDBLoggerWithLevel(level logger.LogLevel) logger.Interface {
	return NewDBLoggerWithLevelOut(level, "sql")
}

func NewDBLoggerWithLevelOut(level logger.LogLevel, logName string) logger.Interface {
	return logger.New(
		util.ZapLogger(logName, "debug", "SQL"),
		logger.Config{
			SlowThreshold:             200 * time.Millisecond,
			Colorful:                  false,
			IgnoreRecordNotFoundError: true,
			ParameterizedQueries:      false,
			LogLevel:                  level,
		},
	)
}

func NewGormDB() *GormDB {
	return &GormDB{
		models:     make(map[string]interface{}),
		viewModels: make(map[string]interface{}),
	}
}

func ParseModel(model interface{}) (s *schema.Schema, err error) {
	s, err = schema.Parse(model, localSyncMap, schema.NamingStrategy{})
	return
}

func SetModelBus(bus interface{}) {
	globalModelBus = bus
}

func defaultModelArgs() *argsTagModel {
	return &argsTagModel{
		AutoDelete: false,
		Save:       "days",
		Val:        90,
	}
}

func mergeModels(m1, m2 map[string]interface{}) (newMap map[string]interface{}) {
	newMap = make(map[string]interface{})
	for k, v := range m1 {
		newMap[k] = v
	}
	for k, v := range m2 {
		newMap[k] = v
	}
	return
}

func modelTagParse(tag string) *argsTagModel {
	if tag == "" {
		return nil
	}
	ret := defaultModelArgs()
	for _, v := range strings.Split(tag, ";") {
		if v == "" {
			continue
		}
		kv := strings.Split(strings.TrimSpace(v), ":")
		k := kv[0]
		switch k {
		case "auto_delete", "autoDelete":
			ret.AutoDelete = true
		case "save":
			if len(kv) < 2 {
				continue
			}
			saveVal := strings.TrimSpace(kv[1])
			if len(kv) >= 2 {
				if saveVal == "count" || saveVal == "days" {
					ret.Save = saveVal
				}
				if saveVal == "count" {
					ret.Val = 100000
				}
				if saveVal == "days" {
					ret.Val = 90
				}
			}
			if len(kv) > 2 {
				val := cast.ToInt(kv[2])
				if val > 0 {
					ret.Val = val
				}
			}
		}
	}
	return ret
}
