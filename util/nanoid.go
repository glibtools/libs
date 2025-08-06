package util

import (
	"github.com/matoous/go-nanoid/v2"
)

const (
	Alphabet = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz"
)

func GenNanoid(n int) string {
	return gonanoid.MustGenerate(Alphabet, n)
}

func GenNanoidNums(ns ...int) string {
	n := 6
	if len(ns) > 0 {
		n = ns[0]
	}
	return gonanoid.MustGenerate("0123456789", n)
}
