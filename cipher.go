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

// Encrypting Keys and Values:
//
// - We expand the input key into distinct keys for enciphering
//   keys and values separately. We also expand this into a
//   shared nonce.
// - Each path segment of a given key-path is encrypted separately
//   with AES-GCM - but with a common nonce for all segments
// - Values are encrypted with a unique and random nonce
// - We store a copy of the full unencrypted key-path along with the
//   plaintext value; both are encrypted and treated as "value".

type encryptor struct {
	val   cipher.AEAD
	key   cipher.AEAD
	nonce []byte
}

// make a new encryptor with the given key
func newEncryptor(key []byte) (*encryptor, error) {
	// first compress the key with sha3 to lengthen potentially short keys
	xpanded := sha3.Sum512(key)
	keymat := expand(32+32+aes.BlockSize, xpanded[:], "DB Encryption Keys")
	defer clear(keymat)

	aekey, keymat := keymat[:32], keymat[32:]
	ctrkey, keymat := keymat[:32], keymat[32:]
	iv := keymat

	blk0, err := aes.NewCipher(aekey)
	if err != nil {
		return nil, fmt.Errorf("aes: %w", err)
	}

	blk1, err := aes.NewCipher(ctrkey)
	if err != nil {
		return nil, fmt.Errorf("aes: %w", err)
	}

	aead0, err := cipher.NewGCM(blk0)
	if err != nil {
		return nil, fmt.Errorf("aes-gcm: %w", err)
	}

	aead1, err := cipher.NewGCM(blk1)
	if err != nil {
		return nil, fmt.Errorf("aes-gcm: %w", err)
	}

	c := &encryptor{
		key:   aead0,
		val:   aead1,
		nonce: iv[:aead0.NonceSize()],
	}
	return c, nil
}

// Encrypt one path segment
func (c *encryptor) encSegment(s string) []byte {
	nm := []byte(s)
	z := make([]byte, len(nm)+c.key.Overhead())

	ct := c.key.Seal(z[:0], c.nonce, nm, nil)
	return ct
}

// Decrypt one path segment
func (c *encryptor) decSegment(v []byte) (string, error) {
	if len(v) < c.key.Overhead() {
		return "", fmt.Errorf("seg: too short (%d)", len(v))
	}

	z := make([]byte, len(v)-c.key.Overhead())
	pt, err := c.key.Open(z[:0], c.nonce, v, nil)
	if err != nil {
		return "", err
	}
	return string(pt), nil
}

// Encrypt the key & values for a given kv pair
func (c *encryptor) encryptKV(k string, v []byte) []byte {
	nl := c.val.NonceSize()
	ov := c.val.Overhead()

	ct := make([]byte, nl+ov+len(k)+len(v)+4)
	nonce, pt := ct[:nl], ct[nl:]

	randfill(nonce)

	z := enc32(pt, len(k))
	z = xcopy(z, k)
	z = xcopy(z, v)
	n := cap(pt) - cap(z)

	z = c.val.Seal(pt[:0], nonce, pt[:n], nil)
	return ct
}

// Decrypt the key, value pair in 'ct'
func (c *encryptor) decryptKV(ct []byte) (string, []byte, error) {
	nl := c.val.NonceSize()
	ov := c.val.Overhead()

	if len(ct) < (nl + ov + 4) {
		return "", nil, fmt.Errorf("aes-gcm decrypt: buf len %d too small", len(ct))
	}

	pt := make([]byte, len(ct)-ov-4)
	nonce, ct := ct[:nl], ct[nl:]

	pt, err := c.val.Open(pt[:0], nonce, ct, nil)
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

// use sha3's XOF to create a HKDF like key expansion function
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
