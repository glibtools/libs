package util

import (
	"crypto/md5"
	"encoding/hex"
	"fmt"
	"strings"

	"github.com/google/uuid"
	"github.com/lithammer/shortuuid/v4"
	gonanoid "github.com/matoous/go-nanoid/v2"
)

const (
	Alphabet = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz"

	LowerAlphanumericAlphabet = "0123456789abcdefghijklmnopqrstuvwxyz"
)

func GenNanoid(n int) string {
	return gonanoid.MustGenerate(Alphabet, n)
}

func GenNanoidLowerAlphanumeric(n int) string {
	return gonanoid.MustGenerate(LowerAlphanumericAlphabet, n)
}

func GenNanoidNums(ns ...int) string {
	n := 6
	if len(ns) > 0 {
		n = ns[0]
	}
	return gonanoid.MustGenerate("0123456789", n)
}

func GenUUID16(sep string, uuidVs ...uuid.UUID) string {
	return fmt.Sprintf("%s%s%s", GenNanoid(18), sep, UUID16md5hex(uuidVs...))
}

func GenUUID16unix(sep string, uuidVs ...uuid.UUID) string {
	return fmt.Sprintf("%d%s%s", TimeNow().Unix(), sep, UUID16md5hex(uuidVs...))
}

func ShortUUID(uuidVs ...uuid.UUID) string {
	return shortuuid.DefaultEncoder.Encode(getUUID(uuidVs...))
}

func UUID16md5hex(uuidVs ...uuid.UUID) string {
	str := getUUID(uuidVs...).String()
	md5New := md5.New()
	md5New.Write([]byte(str))
	b0 := md5New.Sum(nil)[:]
	b0 = b0[4:12]
	str = hex.EncodeToString(b0)
	str = strings.ToUpper(str)
	return str
}

// UUIDString ...
func UUIDString(uuidVs ...uuid.UUID) string { return getUUID(uuidVs...).String() }

func getUUID(uuidVs ...uuid.UUID) uuid.UUID {
	if len(uuidVs) > 0 {
		return uuidVs[0]
	}
	return uuid.Must(uuid.NewUUID())
}
