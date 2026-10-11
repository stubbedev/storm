package orm

import (
	"bytes"
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"database/sql/driver"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"sync/atomic"

	"golang.org/x/crypto/bcrypt"
)

// JSON stores V as JSON text — Eloquent's 'array' / 'object' / AsCollection
// casts. Other casts are plain Go types (time.Time, bool, typed string
// enums) or any type implementing sql.Scanner and driver.Valuer.
type JSON[V any] struct{ V V }

func (j JSON[V]) Value() (driver.Value, error) {
	b, err := json.Marshal(j.V)
	return string(b), err
}

func (j *JSON[V]) Scan(src any) error {
	switch s := src.(type) {
	case nil:
		var zero V
		j.V = zero
		return nil
	case string:
		return j.decode([]byte(s))
	case []byte:
		return j.decode(s)
	}
	b, err := json.Marshal(src)
	if err != nil {
		return fmt.Errorf("orm: cannot scan %T into JSON: %w", src, err)
	}
	return j.decode(b)
}

// decode replaces V with the decoded value rather than merging into it.
func (j *JSON[V]) decode(b []byte) error {
	var v V
	if err := json.Unmarshal(b, &v); err != nil {
		return err
	}
	j.V = v
	return nil
}

func (j JSON[V]) MarshalJSON() ([]byte, error)  { return json.Marshal(j.V) }
func (j *JSON[V]) UnmarshalJSON(b []byte) error { return j.decode(b) }

var encryptionKey atomic.Pointer[[]byte]

// EncryptionKeySize is the length of the key Encrypted seals with: an
// AES-256 key, 32 random bytes.
const EncryptionKeySize = 32

// SetEncryptionKey sets the key Encrypted seals values with: exactly 32
// random bytes (generate them once with crypto/rand and keep them in a
// secret store, e.g. base64 in APP_KEY). Any other length — an empty
// key from an unset environment variable included — is refused.
func SetEncryptionKey(key []byte) error {
	if len(key) != EncryptionKeySize {
		return fmt.Errorf("orm: an encryption key must be %d bytes, got %d", EncryptionKeySize, len(key))
	}
	k := bytes.Clone(key)
	encryptionKey.Store(&k)
	return nil
}

// aead is the cipher Encrypted seals with, keyed by SetEncryptionKey.
func aead() (cipher.AEAD, error) {
	key := encryptionKey.Load()
	if key == nil {
		return nil, errors.New("orm: call orm.SetEncryptionKey before using Encrypted")
	}
	block, err := aes.NewCipher(*key)
	if err != nil {
		return nil, err
	}
	return cipher.NewGCM(block)
}

func sealed(v []byte) (string, error) {
	gcm, err := aead()
	if err != nil {
		return "", err
	}
	nonce := make([]byte, gcm.NonceSize())
	if _, err := io.ReadFull(rand.Reader, nonce); err != nil {
		return "", err
	}
	return base64.StdEncoding.EncodeToString(gcm.Seal(nonce, nonce, v, nil)), nil
}

func opened(s string) ([]byte, error) {
	gcm, err := aead()
	if err != nil {
		return nil, err
	}
	raw, err := base64.StdEncoding.DecodeString(s)
	if err != nil {
		return nil, fmt.Errorf("orm: decrypting value: %w", err)
	}
	if len(raw) < gcm.NonceSize() {
		return nil, errors.New("orm: encrypted value is too short")
	}
	plain, err := gcm.Open(nil, raw[:gcm.NonceSize()], raw[gcm.NonceSize():], nil)
	if err != nil {
		return nil, fmt.Errorf("orm: decrypting value: %w", err)
	}
	return plain, nil
}

// Encrypted stores V as an authenticated AES-GCM encrypted JSON payload —
// Eloquent's 'encrypted' and 'encrypted:array' casts.
//
//	key, err := base64.StdEncoding.DecodeString(os.Getenv("APP_KEY"))
//	err = orm.SetEncryptionKey(key) // 32 random bytes
//	// Secret string `db:"secret"`
//
// Secret orm.Encrypted[map[string]string] `db:"secret"`
type Encrypted[V any] struct{ V V }

func (e Encrypted[V]) Value() (driver.Value, error) {
	b, err := json.Marshal(e.V)
	if err != nil {
		return nil, err
	}
	return sealed(b)
}

func (e *Encrypted[V]) Scan(src any) error {
	var text string
	switch s := src.(type) {
	case nil:
	case string:
		text = s
	case []byte:
		text = string(s)
	default:
		return fmt.Errorf("orm: cannot scan %T into Encrypted", src)
	}
	if text == "" {
		var zero V
		e.V = zero
		return nil
	}
	b, err := opened(text)
	if err != nil {
		return err
	}
	return e.UnmarshalJSON(b)
}

func (e Encrypted[V]) MarshalJSON() ([]byte, error) { return json.Marshal(e.V) }

// UnmarshalJSON replaces V with the decoded value rather than merging.
func (e *Encrypted[V]) UnmarshalJSON(b []byte) error {
	var v V
	if err := json.Unmarshal(b, &v); err != nil {
		return err
	}
	e.V = v
	return nil
}

// Hashed is a bcrypt password hash — Eloquent's 'hashed' cast. It only
// ever holds a hash: HashPassword makes one from a plain password, and
// reading the column loads one, so a model never carries (or serialises)
// the plain password, and a value that is not a bcrypt hash is refused on
// write.
//
//	type User struct {
//		orm.Model
//		Password orm.Hashed `db:"password"`
//	}
//
//	u.Password, err = orm.HashPassword("secret")
//	u.Password.Matches("secret") // true
type Hashed struct{ hash string }

// HashPassword hashes plain with bcrypt at its default cost.
func HashPassword(plain string) (Hashed, error) {
	hash, err := bcrypt.GenerateFromPassword([]byte(plain), bcrypt.DefaultCost)
	if err != nil {
		return Hashed{}, fmt.Errorf("orm: hashing a password: %w", err)
	}
	return Hashed{string(hash)}, nil
}

// IsZero reports whether no password is set.
func (h Hashed) IsZero() bool { return h.hash == "" }

func (h Hashed) Value() (driver.Value, error) {
	if h.hash == "" {
		return nil, nil
	}
	return h.hash, nil
}

func (h *Hashed) Scan(src any) error {
	var s string
	switch v := src.(type) {
	case nil:
		*h = Hashed{}
		return nil
	case string:
		s = v
	case []byte:
		s = string(v)
	default:
		return fmt.Errorf("orm: cannot scan %T into Hashed", src)
	}
	if !isBcrypt(s) {
		return errors.New("orm: the stored value is not a bcrypt hash")
	}
	*h = Hashed{s}
	return nil
}

// Matches reports whether plain verifies against the stored hash.
func (h Hashed) Matches(plain string) bool {
	return h.hash != "" && bcrypt.CompareHashAndPassword([]byte(h.hash), []byte(plain)) == nil
}

// MarshalJSON writes the hash, never a password; UnmarshalJSON accepts
// only a bcrypt hash.
func (h Hashed) MarshalJSON() ([]byte, error) { return json.Marshal(h.hash) }

func (h *Hashed) UnmarshalJSON(b []byte) error {
	var s string
	if err := json.Unmarshal(b, &s); err != nil {
		return err
	}
	if s == "" {
		*h = Hashed{}
		return nil
	}
	return h.Scan(s)
}

func isBcrypt(s string) bool {
	_, err := bcrypt.Cost([]byte(s))
	return len(s) == 60 && err == nil
}
