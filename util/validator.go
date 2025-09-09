package util

import (
	"errors"
	"regexp"
	"strings"
	"unicode"

	lzh "github.com/go-playground/locales/zh"
	ut "github.com/go-playground/universal-translator"
	"github.com/go-playground/validator/v10"
	"github.com/go-playground/validator/v10/translations/zh"
)

var ValidatorInc = new(Validator).New()

// Validator ...
type Validator struct {
	valid *validator.Validate
	trans ut.Translator
}

// Constructor ...
func (v *Validator) Constructor() {
	err := v.initialize()
	if err != nil {
		panic(err)
	}
}

func (v *Validator) Engine() interface{} {
	return v.valid
}

// New ...
func (v *Validator) New() *Validator { v.Constructor(); return v }

// TranslateZh ...
func (v *Validator) TranslateZh(es error) (err error) {
	if es == nil {
		return
	}
	if e := (validator.ValidationErrors{}); errors.As(es, &e) {
		list := make(listError, 0)
		for _, fieldError := range e {
			list = append(list, fieldError.Translate(v.trans))
		}
		err = list
		return
	}
	if e := (*validator.InvalidValidationError)(nil); errors.As(es, &e) {
		err = errors.New("参数错误:" + es.Error())
		return
	}
	err = es
	return
}

// Valid ...
func (v *Validator) Valid() *validator.Validate { return v.valid }

// ValidAndTranslateZh ...
func (v *Validator) ValidAndTranslateZh(s interface{}) (err error) {
	return v.TranslateZh(v.valid.Struct(s))
}

// ValidVarWithTransZh ...
func (v *Validator) ValidVarWithTransZh(val interface{}, tag string) (err error) {
	return v.TranslateZh(v.valid.Var(val, tag))
}

func (v *Validator) ValidateStruct(obj interface{}) error {
	if obj == nil {
		return nil
	}
	return v.ValidAndTranslateZh(obj)
}

// initialize ...
func (v *Validator) initialize() (err error) {
	//中文翻译器
	zh1 := lzh.New()
	trans, _ := ut.New(zh1, zh1).GetTranslator("zh")
	//验证器
	validate := validator.New()
	//验证器注册翻译器
	err = zh.RegisterDefaultTranslations(validate, trans)
	if err != nil {
		return
	}
	v.trans = trans
	v.valid = validate
	customValidations := []func() error{
		v.regPassword,
		v.regTranslationEmail,
		v.regTranslationMobile,
		v.regTranslationUsername,
	}
	for _, fn := range customValidations {
		if err = fn(); err != nil {
			return
		}
	}
	return
}

// regPassword ...
func (v *Validator) regPassword() (err error) {
	_fn1 := func(str string) bool {
		if !regexp.MustCompile(`^\S{8,24}$`).MatchString(str) {
			return false
		}
		if !regexp.MustCompile(`^.*[A-Z]+.*$`).MatchString(str) {
			return false
		}
		if !regexp.MustCompile(`^.*[a-z]+.*$`).MatchString(str) {
			return false
		}
		if !regexp.MustCompile(`^.*\d+.*$`).MatchString(str) {
			return false
		}
		return true
	}
	validSlice := []string{"password", "pwd", "user_password", "user_pwd"}
	for _, valid := range validSlice {
		if err = v.validatorRegValidation(valid, "{0}(密码必须同时包含大小写字母和数字;并且长度为[8-24];不能包含空格)",
			func(fl validator.FieldLevel) bool {
				return _fn1(fl.Field().String())
			},
		); err != nil {
			return
		}
	}
	return
}

// regTranslationEmail ...
func (v *Validator) regTranslationEmail() (err error) {
	validSlice := []string{"email", "mail"}
	for _, valid := range validSlice {
		if err = v.validatorRegValidation(valid, "{0}(邮箱格式错误)",
			func(fl validator.FieldLevel) bool {
				return regexp.MustCompile(`^(\w)+(\.\w+)*@(\w)+((\.\w+)+)$`).MatchString(fl.Field().String())
			},
		); err != nil {
			return
		}
	}
	return
}

// regTranslationMobile ...
func (v *Validator) regTranslationMobile() (err error) {
	validSlice := []string{"mobile", "phone", "tel", "telephone"}
	for _, valid := range validSlice {
		if err = v.validatorRegValidation(valid, "{0}(手机号码错误)",
			func(fl validator.FieldLevel) bool {
				return regexp.MustCompile(`^1[3456789]\d{9}$`).MatchString(fl.Field().String())
			},
		); err != nil {
			return
		}
	}
	return
}

// regTranslationUsername ...
func (v *Validator) regTranslationUsername() (err error) {
	validSlice := []string{"account", "username", "account_name", "user_account"}
	for _, valid := range validSlice {
		if err = v.validatorRegValidation(valid, "{0}(用户账号为字母或与数字组合;并且长度为[5-20])",
			func(fl validator.FieldLevel) bool {
				return validateStringName(fl.Field().String())
			},
		); err != nil {
			return
		}
	}
	return
}

func (v *Validator) validatorRegValidation(tagName, text string, fn validator.Func) (err error) {
	if err = v.valid.RegisterValidation(tagName, fn); err != nil {
		return
	}
	return v.valid.RegisterTranslation(tagName, v.trans, func(ut ut.Translator) error {
		return ut.Add(tagName, text, true)
	}, func(ut ut.Translator, fe validator.FieldError) string {
		t, _ := ut.T(tagName, fe.Field())
		return t
	})
}

type listError []string

func (l listError) Error() string { return strings.Join(l, ",") }

// ValidateStringName ... 验证字符串是否符合规则 (只包含字母和数字, 并且包含至少一个字母和一个数字)
func validateStringName(input string) bool {
	// 检查是否只包含字母和数字
	if !regexp.MustCompile(`^[A-Za-z\d]{5,20}$`).MatchString(input) {
		return false
	}
	//检查第一个字符是否为字母
	if !unicode.IsLetter(rune(input[0])) {
		return false
	}
	// 检查是否包含至少一个字母和一个数字,
	/*hasLetter, hasDigit := false, false
	for _, r := range input {
		switch {
		case unicode.IsLetter(r):
			hasLetter = true
		case unicode.IsDigit(r):
			hasDigit = true
		}
		if hasLetter && hasDigit {
			return true
		}
	}*/
	return true
}
