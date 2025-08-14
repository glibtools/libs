package giris

import (
	"encoding/base64"
	"image/color"
	"strings"
	"sync"

	"github.com/kataras/iris/v12"
	"github.com/mojocn/base64Captcha"
)

const captchaID = "Captcha-ID"

type Captcha struct {
	Driver base64Captcha.Driver
	Store  base64Captcha.Store

	inc  *base64Captcha.Captcha
	once sync.Once
}

// Captcha ......
func (c *Captcha) Captcha() iris.Handler {
	return func(ctx iris.Context) {
		arg, err := c.Generate()
		if err != nil {
			ctx.StatusCode(iris.StatusInternalServerError)
			return
		}
		// png
		ctx.Header("Content-Type", base64Captcha.MimeTypeImage)
		ctx.Header("Cache-Control", "no-cache, no-store, must-revalidate")
		ctx.Header("Pragma", "no-cache")
		ctx.Header("Expires", "0")
		ctx.Header("Access-Control-Expose-Headers", "")
		ctx.Header("Access-Control-Expose-Headers", captchaID)
		ctx.Header(captchaID, arg.ID)
		bts, _ := base64.StdEncoding.DecodeString(arg.Content)
		_, _ = ctx.Write(bts)
	}
}

func (c *Captcha) Generate() (arg CaptchaArg, err error) {
	arg.ID, arg.Content, _, err = c.getInc().Generate()
	if err != nil {
		return
	}
	arg.Content = removeBase64StringPrefix(arg.Content)
	return
}

func (c *Captcha) Verify(arg *CaptchaArg) bool {
	if arg == nil {
		return false
	}
	id, answer := arg.ID, arg.Answer
	if !(id != "" && answer != "") {
		return false
	}
	return c.getInc().Verify(id, answer, true)
}

func (c *Captcha) getInc() *base64Captcha.Captcha {
	c.once.Do(func() {
		c.initInc()
		c.inc = base64Captcha.NewCaptcha(c.Driver, c.Store)
	})
	return c.inc
}

func (c *Captcha) initInc() {
	c.Driver = base64Captcha.NewDriverString(
		80,
		160,
		0,
		base64Captcha.OptionShowSlimeLine,
		4,
		"1234567890",
		&color.RGBA{R: 0, G: 0, B: 0, A: 0},
		base64Captcha.DefaultEmbeddedFonts,
		[]string{"wqy-microhei.ttc"},
	)
	c.Store = base64Captcha.DefaultMemStore
}

type CaptchaArg struct {
	ID      string `json:"id"`
	Answer  string `json:"answer,omitempty"`
	Content string `json:"content,omitempty"`
}

func removeBase64StringPrefix(str string) string {
	findFlag := "base64,"
	i := strings.Index(str, findFlag)
	if i == -1 {
		return str
	}
	return str[i+len(findFlag):]
}
