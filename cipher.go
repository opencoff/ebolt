// cipher.go - encrypt/decrypt routines for bolt KV pairs

package ebolt

import (
	"fmt"

	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"crypto/sha3"
	"encoding/binary"
)

type encryptor struct {
	hkey []byte
	ae   cipher.AEAD
}

// make a new encryptor with the given key
func newEncryptor(key []byte) (*encryptor, error) {
	keymat := expand(32+32, key, "DB Encryption Keys")

	key, keymat = keymat[:32], keymat[32:]
	hkey, keymat := keymat[:32], keymat[32:]

	aes, err := aes.NewCipher(key)
	if err != nil {
		return nil, fmt.Errorf("aes: %w", err)
	}

	ae, err := cipher.NewGCM(aes)
	if err != nil {
		return nil, fmt.Errorf("aes-gcm: %w", err)
	}

	c := &encryptor{
		hkey: hkey,
		ae:   ae,
	}
	return c, nil
}

func (c *encryptor) encKey(k string) []byte {
	h := sha3.New256()
	h.Write(c.hkey)
	h.Write([]byte(k))
	z := h.Sum(nil)
	return z[:]
}

func (c *encryptor) encryptKV(k string, v []byte) []byte {
	nl := c.ae.NonceSize()
	ov := c.ae.Overhead()

	ct := make([]byte, nl+ov+len(k)+len(v)+4)
	nonce, pt := ct[:nl], ct[nl:]

	randfill(nonce)

	z := enc32(pt, len(k))
	z = xcopy(z, k)
	z = xcopy(z, v)
	n := cap(pt) - cap(z)

	z = c.ae.Seal(pt[:0], nonce, pt[:n], nil)
	return ct
}

func (c *encryptor) decryptKV(ct []byte) (string, []byte, error) {
	nl := c.ae.NonceSize()
	ov := c.ae.Overhead()

	if len(ct) < (nl + ov + 4) {
		return "", nil, fmt.Errorf("aes-gcm decrypt: buf len %d too small", len(ct))
	}

	pt := make([]byte, len(ct)-ov-4)
	nonce, ct := ct[:nl], ct[nl:]

	pt, err := c.ae.Open(pt[:0], nonce, ct, nil)
	if err != nil {
		return "", nil, fmt.Errorf("aes-gcm decrypt: %w", err)
	}

	z, kl := dec32[int](pt)
	if len(z) < kl {
		return "", nil, fmt.Errorf("aes-gcm decrypt: pt len %d too small", len(z))
	}

	k := z[:kl]
	v := z[kl:]
	return string(k), v, nil
}

func enc32[T ~int | ~uint | ~int32 | ~uint32](b []byte, v T) []byte {
	binary.BigEndian.PutUint32(b[:4], uint32(v))
	return b[4:]
}

func dec32[T ~int | ~uint | ~int32 | ~uint32](b []byte) ([]byte, T) {
	n := binary.BigEndian.Uint32(b[:4])
	return b[4:], T(n)
}

func xcopy[T ~string | ~[]byte](dst []byte, src T) []byte {
	n := copy(dst, src)
	return dst[n:]
}

func expand(n int, secret []byte, ctx string, ad ...[]byte) []byte {
	h := sha3.NewCSHAKE256(nil, []byte(ctx))
	h.Write(secret)
	for i := range ad {
		h.Write(ad[i])
	}

	out := make([]byte, n)
	h.Read(out)
	return out
}

func randfill(b []byte) []byte {
	if _, err := rand.Read(b); err != nil {
		panic(fmt.Sprintf("rand: %s", err))
	}
	return b
}
