package orm

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"crypto/sha256"
	"database/sql/driver"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"

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
		return json.Unmarshal([]byte(s), &j.V)
	case []byte:
		return json.Unmarshal(s, &j.V)
	}
	b, err := json.Marshal(src)
	if err != nil {
		return fmt.Errorf("orm: cannot scan %T into JSON: %w", src, err)
	}
	return json.Unmarshal(b, &j.V)
}

func (j JSON[V]) MarshalJSON() ([]byte, error)  { return json.Marshal(j.V) }
func (j *JSON[V]) UnmarshalJSON(b []byte) error { return json.Unmarshal(b, &j.V) }

var encryptionKey []byte

// SetEncryptionKey sets the key Encrypted seals values with. Any length is
// accepted; it is stretched to 32 bytes (AES-256). Call it once at startup,
// before the first Encrypted value is stored or read.
func SetEncryptionKey(key []byte) {
	sum := sha256.Sum256(key)
	encryptionKey = sum[:]
}

// aead is the cipher Encrypted seals with, keyed by SetEncryptionKey.
func aead() (cipher.AEAD, error) {
	if encryptionKey == nil {
		return nil, errors.New("orm: call orm.SetEncryptionKey before using Encrypted")
	}
	block, err := aes.NewCipher(encryptionKey)
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
//	orm.SetEncryptionKey([]byte(os.Getenv("APP_KEY")))
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
	return json.Unmarshal(b, &e.V)
}

func (e Encrypted[V]) MarshalJSON() ([]byte, error)  { return json.Marshal(e.V) }
func (e *Encrypted[V]) UnmarshalJSON(b []byte) error { return json.Unmarshal(b, &e.V) }

// Hashed stores a bcrypt hash of the string — Eloquent's 'hashed' cast.
// Assigning a plain password hashes it; assigning a value that is already
// a bcrypt hash stores it as-is, so loaded models can be re-saved without
// double hashing.
//
//	type User struct {
//		orm.Model
//		Password orm.Hashed `db:"password"`
//	}
//
//	u.Password = "secret"
//	u.Password.Matches("secret") // true after saving or assigning
type Hashed string

func (h Hashed) Value() (driver.Value, error) {
	if h == "" || isBcrypt(string(h)) {
		return string(h), nil
	}
	hash, err := bcrypt.GenerateFromPassword([]byte(h), bcrypt.DefaultCost)
	return string(hash), err
}

func (h *Hashed) Scan(src any) error {
	switch s := src.(type) {
	case nil:
		*h = ""
		return nil
	case string:
		*h = Hashed(s)
		return nil
	case []byte:
		*h = Hashed(s)
		return nil
	}
	return fmt.Errorf("orm: cannot scan %T into Hashed", src)
}

// Matches reports whether plain verifies against the stored hash.
func (h Hashed) Matches(plain string) bool {
	return bcrypt.CompareHashAndPassword([]byte(h), []byte(plain)) == nil
}

func isBcrypt(s string) bool {
	return len(s) == 60 && (strings.HasPrefix(s, "$2a$") || strings.HasPrefix(s, "$2b$") || strings.HasPrefix(s, "$2y$"))
}
