package mdb

import (
	"gorm.io/gorm"

	"github.com/glibtools/libs/util"
)

// Compare this snippet from src/libs/mdb/curd.go:

func FindRecords[T any](val T, call func(tx *gorm.DB) *gorm.DB, args ...interface{}) (sliceResult []T, err error) {
	return FindRecordsWithDB(DB.DB, val, call, args...)
}

// FindRecordsWithDB ...
func FindRecordsWithDB[T any](db *gorm.DB, val T, call func(tx *gorm.DB) *gorm.DB, args ...interface{}) (sliceResult []T, err error) {
	sliceResult = make([]T, 0)
	sl := util.SlicePointerValue(val)
	slp := sl.Interface()
	sq := `SELECT {{.table}}.*
FROM ({{.sql}}) a
LEFT JOIN {{.table}} ON a.id={{.table}}.id{{.append}};`
	var appendSq string
	for _, arg := range args {
		switch v := arg.(type) {
		case string:
			if v != "" {
				appendSq += "\n" + v
			}
		}
	}
	sq = util.TextTemplateMustParse(sq, util.Map{
		"sql": db.ToSQL(func(_tx *gorm.DB) *gorm.DB {
			_tx.Logger = NewDBLoggerSilent()
			return call(_tx).Find(slp)
		}),
		"table":  ModelTableName(val),
		"append": appendSq,
	})
	if err = db.Raw(sq).Find(slp).Error; err != nil {
		return
	}
	el := sl.Elem()
	for i := 0; i < el.Len(); i++ {
		sliceResult = append(sliceResult, el.Index(i).Interface().(T))
	}
	return
}
