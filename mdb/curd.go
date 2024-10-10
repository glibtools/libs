package mdb

import (
	"errors"
	"fmt"
	"reflect"
	"strings"

	"github.com/spf13/cast"
	"gorm.io/gorm"

	"github.com/glibtools/libs/j2rpc"
	"github.com/glibtools/libs/util"
)

const (
	defaultPageSize = 10
	maxPageSize     = 100
)

type CurdParams struct {
	TableName         string      `json:"table_name,omitempty"`
	Values            util.Map    `json:"values,omitempty"`
	Where             string      `json:"where,omitempty"`
	Model             interface{} `json:"-"`
	BeforeCall        func(bean interface{})
	Check             func(bean interface{}) (err error)
	CheckBeforeUpdate func(oldVal, newVal interface{}) (err error)
}

func (c *CurdParams) AddUserIDCondition(bean interface{}, userID uint64) {
	var userIDFieldValue = reflect.ValueOf(bean).Elem().FieldByNameFunc(func(s string) bool {
		return strings.ToLower(s) == "userid"
	})
	if userIDFieldValue.IsValid() && userIDFieldValue.CanSet() {
		c.AddWhereCondition(fmt.Sprintf("user_id = %d", userID))
	}
}

// AddWhereCondition ... 添加where条件
func (c *CurdParams) AddWhereCondition(where string) {
	c.Where = strings.TrimSpace(c.Where)
	if c.Where != "" {
		c.Where = c.Where + " AND " + where
		return
	}
	c.Where = where
}

// Create ...添加数据
func (c *CurdParams) Create() (err error) {
	db := DB
	// get bean
	if c.Model == nil {
		c.Model, err = db.GetFindModel(c.TableName)
		if err != nil {
			return
		}
	}
	bean := c.Model
	// set values
	if err = c.Values.ToBean(bean); err != nil {
		return
	}
	// before call
	if c.BeforeCall != nil {
		c.BeforeCall(bean)
	}
	// check
	if c.Check != nil {
		if err = c.Check(bean); err != nil {
			return
		}
	}
	// create
	return db.Transaction(func(tx *gorm.DB) error {
		return tx.Create(bean).Error
	})
}

// Delete ...删除数据
func (c *CurdParams) Delete() (err error) {
	db := DB
	idv, ok := ValueIDUint64(c.Values)
	if !ok {
		return j2rpc.NewError(400, "id未指定")
	}
	// get bean
	if c.Model == nil {
		c.Model, err = db.GetFindModel(c.TableName)
		if err != nil {
			return
		}
	}
	bean := c.Model
	if err = c.Values.ToBean(bean); err != nil {
		return
	}
	if err = db.Where("id = ?", idv).Take(bean).Error; err != nil {
		return j2rpc.NewError(400, err.Error())
	}
	if c.BeforeCall != nil {
		c.BeforeCall(bean)
	}
	if c.Check != nil {
		if err = c.Check(bean); err != nil {
			return
		}
	}
	// delete
	return db.Transaction(func(tx *gorm.DB) error {
		tx = tx.Model(bean).Where("id=?", idv)
		if c.Where != "" {
			tx = tx.Where(c.Where)
		}
		return tx.Delete(bean).Error
	})
}

// Update ...更新数据
func (c *CurdParams) Update() (err error) {
	db := DB
	idUint64, ok := ValueIDUint64(c.Values)
	if !ok {
		return j2rpc.NewError(400, "id未指定")
	}
	// get bean
	if c.Model == nil {
		c.Model, err = db.GetFindModel(c.TableName)
		if err != nil {
			return
		}
	}
	oldVal := util.NewValue(c.Model)
	// get from db
	if err = db.Where("id = ?", idUint64).Take(oldVal).Error; err != nil {
		return j2rpc.NewError(400, err.Error())
	}
	newVal := util.NewValue(c.Model)
	if err = c.Values.ToBean(newVal); err != nil {
		return
	}
	if c.BeforeCall != nil {
		c.BeforeCall(newVal)
	}
	// check
	if c.CheckBeforeUpdate != nil {
		if err = c.CheckBeforeUpdate(oldVal, newVal); err != nil {
			return
		}
	}
	// if force update
	if v, y := c.Values["force_update"]; y && v == true {
		delete(c.Values, "id")
		delete(c.Values, "force_update")
		fields := make([]string, 0)
		util.BeanHasFieldCallback(newVal, "Version", func(v reflect.Value) {
			//get an old version
			oldVersion := reflect.ValueOf(oldVal).Elem().FieldByName("Version")
			//set a new version
			v.Set(oldVersion)
			fields = append(fields, "version")
		})
		for k := range c.Values {
			fields = append(fields, k)
		}
		return db.Transaction(func(tx *gorm.DB) error {
			tx = tx.Model(newVal)
			if c.Where != "" {
				tx = tx.Where(c.Where)
			}
			return tx.Select(fields).Updates(newVal).Error
		})
	}
	if err = util.MergeBean(oldVal, newVal); err != nil {
		return
	}
	// update
	return db.Transaction(func(tx *gorm.DB) error {
		tx = tx.Model(oldVal)
		if c.Where != "" {
			tx = tx.Where(c.Where)
		}
		return tx.Select("*").Updates(oldVal).Error
	})
}

type FindByID struct {
	TableName string `json:"table_name,omitempty"`
	ID        uint64 `json:"id,omitempty"`
	Condition string `json:"condition,omitempty"`

	Dest interface{} `json:"-"`
}

// AppendEqCondition ...
func (f *FindByID) AppendEqCondition(str string) {
	if f.Condition == "" {
		f.Condition = str
		return
	}
	f.Condition += " AND " + str
}

// FindByID ...
func (f *FindByID) FindByID() (result interface{}, err error) {
	db := DB
	// get bean
	if f.Dest == nil {
		f.Dest, err = db.GetFindModel(f.TableName)
		if err != nil {
			return
		}
	}
	// find
	bean := f.Dest
	tx := db.DB.Model(bean)
	if f.Condition != "" {
		tx = tx.Where(f.Condition)
	}
	err = tx.Take(bean, f.ID).Error
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			err = j2rpc.NewError(404, "数据不存在")
		}
		return
	}
	// after find
	if impl, ok := bean.(ImplResultAfterFind); ok {
		if err = impl.ResultAfterFind(db.DB.Session(&gorm.Session{NewDB: true})); err != nil {
			return
		}
	}
	result = bean
	return
}

type (
	FindParams struct {
		Table     string `json:"table,omitempty"`
		Condition string `json:"condition,omitempty"`
		Order     string `json:"order,omitempty"`
		PageIndex int    `json:"page_index,omitempty"`
		PageSize  int    `json:"page_size,omitempty"`

		Dest interface{} `json:"-"`
	}
	Pagination struct {
		// Total number of records
		Total int64 `json:"total,omitempty"`
		// Size of each page
		Size int `json:"size,omitempty"`
		// Index of pages
		Index int `json:"index,omitempty"`
		// Pages Number of pages
		Pages int `json:"pages,omitempty"`
	}
	FindResult struct {
		// Data
		Data interface{} `json:"data,omitempty"`
		// Pagination
		Pagination *Pagination `json:"pagination,omitempty"`
		// Extra
		Extra interface{} `json:"extra,omitempty"`
	}
)

// AppendEqCondition ...
func (f *FindParams) AppendEqCondition(str string) {
	if f.Condition == "" {
		f.Condition = str
		return
	}
	f.Condition += " AND " + str
}

// FindResultWithModel get data
func (f *FindParams) FindResultWithModel(tx *gorm.DB) (result *FindResult, err error) {
	if f.Table == "" {
		err = errors.New("params.Table is empty")
		return
	}
	if f.Dest == nil {
		dest, e := DB.GetFindModel(f.Table)
		if e != nil {
			err = e
			return
		}
		f.Dest = dest
	}

	tx = f.prepareTx(tx)
	// pagination
	pagination := &Pagination{Size: f.PageSize, Index: 1}
	if err = tx.Count(&pagination.Total).Error; err != nil {
		return
	}
	tx.Order(f.Order)
	// pages
	if pagination.Total > 0 {
		pagination.Pages = int(pagination.Total) / f.PageSize
		if int(pagination.Total)%f.PageSize > 0 {
			pagination.Pages++
		}
	}
	// pagination
	if f.PageIndex > 1 {
		pagination.Index = f.PageIndex
		tx = tx.Offset((f.PageIndex - 1) * f.PageSize)
	}
	tx = tx.Limit(f.PageSize)
	appendSq := ""
	if len(f.Order) > 0 {
		appendSq += "ORDER BY " + f.Order
	}
	data, err := FindRecordsWithDB(
		tx,
		util.NewValue(f.Dest),
		func(_tx *gorm.DB) *gorm.DB { return _tx.Select("id") },
		appendSq,
	)
	if err != nil {
		return
	}
	sl := reflect.ValueOf(data)
	for i := 0; i < sl.Len(); i++ {
		item := sl.Index(i).Interface()
		if impl, ok := item.(ImplResultAfterFind); ok {
			if err = impl.ResultAfterFind(tx.Session(&gorm.Session{NewDB: true})); err != nil {
				return
			}
		}
	}
	result = &FindResult{Data: data, Pagination: pagination}
	return
}

// prepareTx
func (f *FindParams) prepareTx(db *gorm.DB) (tx *gorm.DB) {
	tx = db.Model(f.Dest).Table(f.Table).Select("*")
	if f.PageSize <= 0 {
		f.PageSize = defaultPageSize
	}
	if f.PageSize > maxPageSize {
		f.PageSize = maxPageSize
	}
	if f.Order == "" {
		f.Order = "id DESC"
	}
	// where condition
	if f.Condition != "" {
		tx = tx.Where(f.Condition)
	}
	return
}

type ImplResultAfterFind interface {
	ResultAfterFind(tx *gorm.DB) error
}

func ValueIDUint64(val util.Map) (id uint64, ok bool) {
	idv, ok := val["id"]
	id = cast.ToUint64(idv)
	ok = ok && id != 0
	return
}
