package utils

import (
	"bytes"
	"crypto/rand"
	"fmt"
	"strings"
)

func RandomID() string {
	b := make([]byte, 16)
	_, _ = rand.Read(b)
	return fmt.Sprintf("%x", b)
}

func ReadLine(r *bytes.Reader) (string, error) {
    var sb strings.Builder
    for {
        b, err := r.ReadByte()
        if err != nil {
            return "", err
        }
        if b == '\r' {
            b2, err := r.ReadByte()
            if err != nil {
                return "", err
            }
            if b2 != '\n' {
                return "", fmt.Errorf("expected LF after CR")
            }
            break
        }
        sb.WriteByte(b)
    }
    return sb.String(), nil
}
