package mdb

import (
	"errors"
	"fmt"
	"reflect"
	"slices"
	"strings"

	"github.com/spf13/cast"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"

	"github.com/glibtools/libs/j2rpc"
	"github.com/glibtools/libs/util"
)

const (
	defaultPageSize = 10
	maxPageSize     = 100
)

type CurdParams struct {
	Table             string      `json:"table,omitempty"`
	Values            util.Map    `json:"values,omitempty"`
	Where             string      `json:"where,omitempty"`
	Model             interface{} `json:"-"`
	BeforeCall        func(bean interface{})
	Check             func(bean interface{}) (err error)
	CheckBeforeUpdate func(oldVal, newVal interface{}) (err error)
	DBWrapper         WrapperDBFunc

	AfterCall func(bean interface{}) (err error)
}

func (c *CurdParams) AddUserIDCondition(bean interface{}, userID uint64) {
	var userIDFieldValue = reflect.ValueOf(bean).Elem().FieldByNameFunc(func(s string) bool {
		return strings.EqualFold(s, "userid")
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
func (c *CurdParams) Create(args ...any) (err error) {
	bean, err := c.getModel()
	if err != nil {
		return
	}
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
	err = c.prepareDB(args...).Transaction(func(tx *gorm.DB) error {
		return c.wrapper(tx).Create(bean).Error
	})
	//duplicate error wrap
	if errors.Is(err, gorm.ErrDuplicatedKey) {
		err = j2rpc.NewError(409, "数据已存在")
	}
	if err != nil {
		return
	}
	// after create
	if c.AfterCall != nil {
		return c.AfterCall(bean)
	}
	return
}

// Delete ...删除数据
func (c *CurdParams) Delete(args ...any) (err error) {
	idv, ok := ValueIDUint64(c.Values)
	if !ok {
		return j2rpc.NewError(400, "id 未指定")
	}
	bean, err := c.getModel()
	if err != nil {
		return
	}
	if err = c.Values.ToBean(bean); err != nil {
		return
	}
	if err = DB.Where("id = ?", idv).Take(bean).Error; err != nil {
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
	err = c.prepareDB(args...).Transaction(func(tx *gorm.DB) error {
		return dbWithWhere(c.wrapper(tx).Model(bean).Where("id=?", idv), c.Where).Delete(bean).Error
	})
	if err != nil {
		return
	}
	if c.AfterCall != nil {
		return c.AfterCall(bean)
	}
	return
}

// Update ...更新数据
func (c *CurdParams) Update(args ...any) (err error) {
	idUint64, ok := ValueIDUint64(c.Values)
	if !ok {
		return j2rpc.NewError(400, "id未指定")
	}
	bean, err := c.getModel()
	if err != nil {
		return err
	}
	oldBeanData := util.NewValue(bean)
	if err = DB.Where("id = ?", idUint64).Take(oldBeanData).Error; err != nil {
		return j2rpc.NewError(400, err.Error())
	}

	mps := make(map[string]interface{})
	columns := ModelColumns(oldBeanData)
	selectColumns := make([]string, 0)
	for k, v := range c.Values {
		if slices.Contains(columns, k) && k != "id" {
			mps[k] = v
			selectColumns = append(selectColumns, k)
		}
	}
	if len(selectColumns) == 0 {
		return j2rpc.NewError(400, "更新数据参数为空")
	}

	newBean := util.NewValue(bean)
	if c.BeforeCall != nil {
		c.BeforeCall(newBean)
	}
	if err = util.Map(mps).ToBean(newBean); err != nil {
		return
	}

	// check
	if c.CheckBeforeUpdate != nil {
		if err = c.CheckBeforeUpdate(oldBeanData, newBean); err != nil {
			return
		}
	}

	err = c.prepareDB(args...).Transaction(func(tx *gorm.DB) error {
		return dbWithWhere(c.wrapper(tx), c.Where).Model(oldBeanData).Select(selectColumns).Omit("id").
			Clauses(clause.Returning{}).
			Updates(newBean).Error
	})
	if err != nil {
		return
	}
	if c.AfterCall != nil {
		return c.AfterCall(newBean)
	}
	return
}

func (c *CurdParams) getModel() (interface{}, error) {
	if c.Model == nil {
		model, err := DB.GetFindModel(c.Table)
		if err != nil {
			return nil, err
		}
		c.Model = model
	}
	return c.Model, nil
}

func (c *CurdParams) prepareDB(args ...any) *gorm.DB {
	db := DB.DB
	for _, _arg := range args {
		switch _v := _arg.(type) {
		case WrapperDBFunc:
			db = _v(db)
		case *gorm.DB:
			db = _v
		default:
		}
	}
	return db
}

func (c *CurdParams) wrapper(tx *gorm.DB) *gorm.DB {
	if c.DBWrapper != nil {
		return c.DBWrapper(tx)
	}
	return tx
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
	if f.Dest == nil {
		if f.Table == "" {
			err = errors.New("params.Table is empty")
			return
		}
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
	tx = db.Model(f.Dest).Select("*")
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

type WrapperDBFunc func(db *gorm.DB) *gorm.DB

func ValueIDUint64(val util.Map) (id uint64, ok bool) {
	idv, ok := val["id"]
	id = cast.ToUint64(idv)
	ok = ok && id != 0
	return
}

func dbWithWhere(db *gorm.DB, where string) *gorm.DB {
	if where != "" {
		return db.Where(where)
	}
	return db
}
