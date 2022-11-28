package util

import (
	"math/rand"
	"sync"
	"time"
)

const charset = "abcdefghijklmnopqrstuvwxyz" +
	"ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"

var (
	seededRand = rand.New(rand.NewSource(time.Now().UnixNano()))
	mu         sync.Mutex
)

func StringWithCharset(length int, charset string) string {
	b := make([]byte, length)
	for i := range b {
		b[i] = charset[Intn(len(charset))]
	}
	return string(b)
}

func String(length int) string {
	return StringWithCharset(length, charset)
}

func Intn(n int) int {
	mu.Lock()
	res := seededRand.Intn(n)
	mu.Unlock()
	return res
}
