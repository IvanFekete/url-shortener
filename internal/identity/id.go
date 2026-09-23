package identity

import (
	"crypto/rand"
	"fmt"
)

func Code() (string, error) {
	const alphabet = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz"
	var out [10]byte
	for i := 0; i < len(out); {
		var b [16]byte
		if _, err := rand.Read(b[:]); err != nil {
			return "", err
		}
		for _, v := range b {
			if v >= 248 {
				continue
			}
			out[i] = alphabet[int(v)%62]
			i++
			if i == len(out) {
				break
			}
		}
	}
	return string(out[:]), nil
}
func UUID() (string, error) {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", err
	}
	b[6] = (b[6] & 15) | 64
	b[8] = (b[8] & 63) | 128
	return fmt.Sprintf("%x-%x-%x-%x-%x", b[:4], b[4:6], b[6:8], b[8:10], b[10:]), nil
}
func ValidCode(code string) bool {
	if len(code) != 10 {
		return false
	}
	for _, c := range code {
		if !(c >= '0' && c <= '9' || c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z') {
			return false
		}
	}
	return true
}
