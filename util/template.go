package util

import (
	"bytes"
	"errors"
	"reflect"
	"text/template"
)

var ErrInvalidType = errors.New("invalid type")

// TextTemplateMustParse ...
func TextTemplateMustParse(text string, data interface{}) (result string) {
	var err error
	defer func() {
		if err != nil {
			panic(err)
		}
	}()
	rv := ReflectIndirect(data)
	var val interface{}
	switch rv.Type().Kind() {
	case reflect.Map:
		val = data
	case reflect.Struct:
		val = Bean2Map(data)
	default:
		err = ErrInvalidType
		return
	}
	tp, err := template.New("t").Parse(text)
	if err != nil {
		return
	}
	var buf bytes.Buffer
	if err = tp.Execute(&buf, val); err != nil {
		return
	}
	return buf.String()
}
