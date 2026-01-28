package util

import (
	"database/sql/driver"
	"fmt"
	"strings"
	"time"
)

/*func init() {
	//设定时区,shanghai
	_ = SetTimeZone()
}*/

const (
	//TimeZone ...
	TimeZone = "Asia/Shanghai"
	//Custom ...
	Custom = "2006-01-02 15:04:05"
	//DateLayout ...
	DateLayout = "2006-01-02"
)

// TimeNowFunc ...
var TimeNowFunc = time.Now

// JSONTime ...
type JSONTime time.Time

// Add ...
func (p *JSONTime) Add(d time.Duration) *JSONTime { return NewJSONTimeOfTime(p.Time().Add(d)) }

// Convert2Time ...
func (p *JSONTime) Convert2Time() time.Time {
	if p == nil {
		return time.Time{}
	}
	return time.Time(*p).Local()
}

// Date ...返回一个日期0点的时间
func (p *JSONTime) Date() *JSONTime {
	y, m, d := p.Time().Date()
	dt := time.Date(y, m, d, 0, 0, 0, 0, time.Local)
	t := JSONTime(dt)
	return &t
}

// FromDB ...
func (p *JSONTime) FromDB(data []byte) error {
	timeStd, _ := time.ParseInLocation(Custom, string(data), time.Local)
	*p = JSONTime(timeStd)
	return nil
}

// GobDecode implements the gob.GobDecoder interface.
func (p *JSONTime) GobDecode(data []byte) error {
	s := p.Convert2Time()
	err := (&s).UnmarshalBinary(data)
	if err != nil {
		return err
	}
	*p = JSONTime(s)
	return nil
}

// GobEncode implements the gob.GobEncoder interface.
func (p *JSONTime) GobEncode() ([]byte, error) {
	return p.Convert2Time().MarshalBinary()
}

// MarshalJSON ...
func (p *JSONTime) MarshalJSON() ([]byte, error) {
	if time.Time(*p).IsZero() {
		return []byte(`""`), nil
	}
	data := make([]byte, 0)
	data = append(data, '"')
	data = p.Convert2Time().AppendFormat(data, Custom)
	data = append(data, '"')
	return data, nil
}

// Scan the value of time.Time
func (p *JSONTime) Scan(v interface{}) error {
	value, ok := v.(time.Time)
	if ok {
		*p = JSONTime(value)
		return nil
	}
	return fmt.Errorf("can not convert %v to timestamp", v)
}

// SetByTime ...
func (p *JSONTime) SetByTime(timeVal time.Time) {
	*p = JSONTime(timeVal)
}

// String ...
func (p *JSONTime) String() string { return p.Convert2Time().Format(Custom) }

// StringFormat 转换为固定格式字符串
func (p *JSONTime) StringFormat(layout string) string { return p.Convert2Time().Format(layout) }

// Time ...
func (p *JSONTime) Time() time.Time {
	return p.Convert2Time()
}

// ToDB ...
func (p *JSONTime) ToDB() (b []byte, err error) {
	b = []byte(p.String())
	return
}

// UnmarshalJSON ...
func (p *JSONTime) UnmarshalJSON(data []byte) error {
	formats := []string{
		"2006-01-02 15:04:05",
		"2006-01-02",
		time.RFC3339,
		time.RFC1123,
		time.RFC822,
		"2006-01-02T15:04:05Z07:00",
	}
	str := string(data)
	str = strings.Trim(str, `"`)
	var t time.Time
	var err error
	for _, layout := range formats {
		t, err = time.ParseInLocation(layout, str, time.Local)
		if err == nil {
			*p = JSONTime(t)
			return nil
		}
	}
	*p = JSONTime(time.Time{})
	return fmt.Errorf("UnmarshalJSON: unsupported time format: %s", str)
}

// Value insert timestamp into Mysql needs this function.
func (p *JSONTime) Value() (driver.Value, error) {
	var zeroTime time.Time
	var ti = p.Convert2Time()
	if ti.UnixNano() == zeroTime.UnixNano() {
		return nil, nil
	}
	return ti, nil
}

// Must2JSONTimeAddr maybe panic
func Must2JSONTimeAddr(in string) *JSONTime {
	j, err := ToDatetime(in)
	if err != nil {
		panic(err)
	}
	return &j
}

// NewJSONTimeOfTime 时间转换为JSONTime
func NewJSONTimeOfTime(t time.Time) *JSONTime {
	jsonTime := JSONTime(t)
	return &jsonTime
}

// Now 当前时间
func Now() *JSONTime {
	return NewJSONTimeOfTime(TimeNow())
}

// RetryDo 重试行为
func RetryDo(fn func() error, intervalSecond int64) error {
	var (
		err error
		a   = 0
	)
	for {
		err = fn()
		if err == nil || a > 10 {
			break
		}
		a++
		time.Sleep(time.Second * time.Duration(intervalSecond))
	}
	return err
}

// RetryDoTimes ...
func RetryDoTimes(times, intervalSecond int64, fn func() error) error {
	var a int64
	var err error
	for {
		err = fn()
		if err == nil || a > times {
			break
		}
		a++
		time.Sleep(time.Second * time.Duration(intervalSecond))
	}
	return err
}

// SetTimeZone ...Shanghai
func SetTimeZone() error {
	lc, err := time.LoadLocation(TimeZone)
	if err == nil {
		time.Local = lc
	}
	return err
}

// Ticker ...
func Ticker(d time.Duration, fn func()) {
	tk := time.NewTicker(d)
	defer tk.Stop()
	for range tk.C {
		fn()
	}
}

// TimeExcWrap 包装执行时间
func TimeExcWrap(fn func()) time.Duration {
	n := TimeNow()
	fn()
	return time.Since(n)
}

// TimeNow ...
func TimeNow() time.Time { return TimeNowFunc() }

// ToDatetime ...
func ToDatetime(in string) (JSONTime, error) {
	out, err := time.ParseInLocation(Custom, in, time.Local)
	return JSONTime(out), err
}

// Today ...今日日期
func Today() *JSONTime {
	return NewJSONTimeOfTime(TimeNow()).Date()
}

// TodayDate ...
func TodayDate() string { return TimeNow().Format(DateLayout) }
