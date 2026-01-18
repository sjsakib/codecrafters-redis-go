package redis

import (
	"fmt"
	"crypto/rand"
)

func randomID() string {
	b := make([]byte, 16)
	_, _ = rand.Read(b)
	return fmt.Sprintf("%x", b)
}