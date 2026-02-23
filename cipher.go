// cipher.go - encrypt/decrypt routines for bolt KV pairs

package ebolt

import (
	"fmt"

	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"crypto/sha3"
	"encoding/binary"

	SIV "github.com/jedisct1/go-aes-siv"
)

// Encrypting Keys and Values:
//
// - We expand the input key into distinct keys for enciphering
//   keys and values separately. We also expand this into a
//   shared nonce.
// - Each path segment of a given key-path is encrypted separately
//   with AES-SIV and we use a passphrase derived "AD"
// - Values are encrypted with a unique and random nonce
// - We store a copy of the full unencrypted key-path along with the
//   plaintext value; both are encrypted and treated as "value".

type encryptor struct {
	kv  cipher.AEAD
	seg *SIV.AESSIV

	// We use the IV as the AD for SIV mode
	segAd []byte
}

// make a new encryptor with the given key
func newEncryptor(key []byte) (*encryptor, error) {
	keymat := expand(32+32+aes.BlockSize, key[:], "DB Encryption Keys")
	defer clear(keymat)

	kvkey, keymat := keymat[:32], keymat[32:]
	segkey, keymat := keymat[:32], keymat[32:]
	iv := keymat

	blk0, err := aes.NewCipher(kvkey)
	if err != nil {
		return nil, fmt.Errorf("aes: %w", err)
	}

	siv, err := SIV.New(segkey)
	if err != nil {
		return nil, fmt.Errorf("aes: siv: %w", err)
	}

	kv, err := cipher.NewGCM(blk0)
	if err != nil {
		return nil, fmt.Errorf("aes-gcm: %w", err)
	}

	c := &encryptor{
		kv:    kv,
		seg:   siv,
		segAd: iv[:kv.NonceSize()],
	}
	return c, nil
}

// Encrypt one path segment
func (c *encryptor) encSegment(s string) []byte {
	nm := []byte(s)
	z := make([]byte, len(nm)+c.seg.Overhead())

	ct := c.seg.Seal(z[:0], nil, nm, c.segAd)
	return ct
}

// Decrypt one path segment
func (c *encryptor) decSegment(v []byte) (string, error) {
	if len(v) < c.seg.Overhead() {
		return "", fmt.Errorf("seg: too short (%d)", len(v))
	}

	z := make([]byte, len(v)-c.seg.Overhead())
	pt, err := c.seg.Open(z[:0], nil, v, c.segAd)
	if err != nil {
		return "", err
	}
	return string(pt), nil
}

// Encrypt the key & values for a given kv pair
func (c *encryptor) encryptKV(k string, v []byte) []byte {
	nl := c.kv.NonceSize()
	ov := c.kv.Overhead()

	ct := make([]byte, nl+ov+len(k)+len(v)+4)
	nonce, pt := ct[:nl], ct[nl:]

	randfill(nonce)

	z := enc32(pt, len(k))
	z = xcopy(z, k)
	z = xcopy(z, v)
	n := cap(pt) - cap(z)

	z = c.kv.Seal(pt[:0], nonce, pt[:n], nil)
	return ct
}

// Decrypt the key, value pair in 'ct'
func (c *encryptor) decryptKV(ct []byte) (string, []byte, error) {
	nl := c.kv.NonceSize()
	ov := c.kv.Overhead()

	if len(ct) < (nl + ov + 4) {
		return "", nil, fmt.Errorf("aes-gcm decrypt: buf len %d too small", len(ct))
	}

	pt := make([]byte, len(ct)-ov-4)
	nonce, ct := ct[:nl], ct[nl:]

	pt, err := c.kv.Open(pt[:0], nonce, ct, nil)
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
