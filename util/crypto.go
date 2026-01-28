package util

import (
	"crypto/md5"
	"encoding/hex"
	"errors"
	"strings"

	"github.com/andeya/goutil"
)

const key = "NYwxF4uj8NVvXxawnML5tyVeLmv6eCrA"

var ErrPassword = errors.New("密码错误")
var innerCrypto = NewCrypto(TrimKey(key)[:32])

// Crypto ...
type Crypto struct {
	key string
}

// Decrypt ...
func (c *Crypto) Decrypt(ciphertext string) (string, error) {
	v, err := goutil.AESDecrypt([]byte(c.Key()), []byte(ciphertext))
	if err != nil {
		return "", err
	}
	return string(v), nil
}

// DecryptCBC ...
func (c *Crypto) DecryptCBC(ciphertext string) (string, error) {
	v, err := goutil.AESCBCDecrypt([]byte(c.Key()), []byte(ciphertext))
	if err != nil {
		return "", err
	}
	return string(v), nil
}

// Encrypt ...
func (c *Crypto) Encrypt(val string) string {
	return string(goutil.AESEncrypt([]byte(c.Key()), []byte(val)))
}

// EncryptCBC ...
func (c *Crypto) EncryptCBC(val string) string {
	return string(goutil.AESCBCEncrypt([]byte(c.Key()), []byte(val)))
}

// Key ...
func (c *Crypto) Key() string {
	c.key = strings.TrimSpace(strings.Replace(c.key, "-", "", -1))
	return c.key
}

// CheckPassword ...c cipher_password
// p plaintext
func CheckPassword(p, c string) error {
	s, err := DecryptPassword(c)
	if err != nil {
		return err
	}
	if s != p {
		return ErrPassword
	}
	return nil
}

// DecryptPassword ...
func DecryptPassword(p string) (s string, err error) {
	return innerCrypto.Decrypt(strings.ToLower(p))
}

// EncryptPassword ...
func EncryptPassword(p string) string {
	return strings.ToUpper(innerCrypto.Encrypt(p))
}

func MD5(s string) string {
	md5New := md5.New()
	md5New.Write([]byte(s))
	b0 := md5New.Sum(nil)[:]
	return hex.EncodeToString(b0)
}

// NewCrypto ...
func NewCrypto(key string) *Crypto {
	return &Crypto{key: key}
}

// SetInnerCrypto ... must replace your own crypto
func SetInnerCrypto(k string) {
	innerCrypto = NewCrypto(TrimKey(k)[:32])
}

func TrimKey(k string) string {
	return strings.TrimSpace(strings.Replace(k, "-", "", -1))
}
