package crypto

import (
	"bytes"
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"io"
)

// AESCBCDecrypt hex decodes a piece of data and then decrypts it using CBC mode.
func AESCBCDecrypt(cipherKey, ciphertext []byte, useBase64 ...bool) ([]byte, error) {
	ciphertext, err := decode(ciphertext, useBase64)
	if err != nil {
		return nil, err
	}
	if len(ciphertext) < aes.BlockSize {
		return nil, errors.New("ciphertext too short")
	}
	iv := ciphertext[:aes.BlockSize]
	ciphertext = ciphertext[aes.BlockSize:]
	if len(ciphertext)%aes.BlockSize != 0 {
		return nil, errors.New("ciphertext is not a multiple of the block size")
	}
	block, err := aes.NewCipher(cipherKey)
	if err != nil {
		return nil, err
	}
	mode := cipher.NewCBCDecrypter(block, iv)
	plaintext := ciphertext
	mode.CryptBlocks(plaintext, ciphertext)
	return pkcs5UnPadding(plaintext)
}

// AESCBCEncrypt uses CBC mode to encrypt a piece of data and then encodes it in hex.
func AESCBCEncrypt(cipherKey, plaintext []byte, useBase64 ...bool) ([]byte, error) {
	block, err := aes.NewCipher(cipherKey)
	if err != nil {
		return nil, err
	}
	blockSize := block.BlockSize()
	plaintext = pkcs5Padding(plaintext, blockSize)
	ciphertext := make([]byte, aes.BlockSize+len(plaintext))
	iv := ciphertext[:aes.BlockSize]
	if _, err = io.ReadFull(rand.Reader, iv); err != nil {
		return nil, err
	}
	mode := cipher.NewCBCEncrypter(block, iv)
	mode.CryptBlocks(ciphertext[aes.BlockSize:], plaintext)
	return encode(ciphertext, useBase64), nil
}

// OneBool try to return the first element, otherwise return zero value.
func OneBool(b []bool) bool {
	if len(b) > 0 {
		return b[0]
	}
	return false
}

func base64Decode(src []byte) ([]byte, error) {
	dst := make([]byte, base64.RawURLEncoding.DecodedLen(len(src)))
	n, err := base64.RawURLEncoding.Decode(dst, src)
	return dst[:n], err
}

func base64Encode(src []byte) []byte {
	buf := make([]byte, base64.RawURLEncoding.EncodedLen(len(src)))
	base64.RawURLEncoding.Encode(buf, src)
	return buf
}

func decode(src []byte, useBase64 []bool) ([]byte, error) {
	if OneBool(useBase64) {
		return base64Decode(src)
	}
	return hexDecode(src)
}

func encode(src []byte, useBase64 []bool) []byte {
	if OneBool(useBase64) {
		return base64Encode(src)
	}
	return hexEncode(src)
}

func hexDecode(src []byte) ([]byte, error) {
	dst := make([]byte, hex.DecodedLen(len(src)))
	_, err := hex.Decode(dst, bytes.ToLower(src))
	return dst, err
}

func hexEncode(src []byte) []byte {
	dst := make([]byte, hex.EncodedLen(len(src)))
	hex.Encode(dst, src)
	return bytes.ToUpper(dst)
}

func pkcs5Padding(plaintext []byte, blockSize int) []byte {
	n := byte(blockSize - len(plaintext)%blockSize)
	for i := byte(0); i < n; i++ {
		plaintext = append(plaintext, n)
	}
	return plaintext
}

func pkcs5UnPadding(r []byte) ([]byte, error) {
	l := len(r)
	if l == 0 {
		return nil, errors.New("input padded bytes is empty")
	}
	last := int(r[l-1])
	if l-last < 0 {
		return nil, errors.New("input padded bytes is invalid")
	}
	n := byte(last)
	pad := r[l-last : l]
	isPad := true
	for _, v := range pad {
		if v != n {
			isPad = false
			break
		}
	}
	if !isPad {
		return nil, errors.New("remove pad error")
	}
	return r[:l-last], nil
}
