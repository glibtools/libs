package util

import (
	"time"
)

func Ticker(inverval time.Duration, fn func()) {
	ticker := time.NewTicker(inverval)
	defer ticker.Stop()
	for range ticker.C {
		fn()
	}
}
