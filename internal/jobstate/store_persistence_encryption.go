package jobstate

import (
	"context"
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"os"

	"github.com/zilliztech/milvus-backup/internal/cfg"
)

// KeyProvider wraps per-write data keys. Only the provider accesses its master key.
type KeyProvider interface {
	Wrap(context.Context, []byte) (WrappedKey, error)
	Unwrap(context.Context, WrappedKey) ([]byte, error)
}

type WrappedKey struct {
	KeyID      string `json:"keyID"`
	Ciphertext []byte `json:"ciphertext"`
}

type aesKeyProvider struct {
	id     string
	cipher cipher.AEAD
}

func aes256GCM(key []byte) (cipher.AEAD, error) {
	if len(key) != 32 {
		return nil, errors.New("persistence encryption: AES-256 requires a 32-byte key")
	}
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, err
	}
	return cipher.NewGCMWithRandomNonce(block)
}

const (
	envMasterKeyID = "RESTORE_PERSISTENCE_ENCRYPTION_KEY_ID"
	envMasterKey   = "RESTORE_PERSISTENCE_ENCRYPTION_KEY"
)

func newKeyProvider(c cfg.PersistenceEncryptionConfig) (KeyProvider, error) {
	var id, encoded string
	switch c.Provider.Val {
	case "config":
		id, encoded = c.Config.KeyID.Val, c.Config.Key.Val
		if id == "" {
			return nil, errors.New("persistence encryption: keyID is required")
		}
	case "env":
		id, encoded = os.Getenv(envMasterKeyID), os.Getenv(envMasterKey)
		var missing error
		if id == "" {
			missing = fmt.Errorf("persistence encryption: %s is required", envMasterKeyID)
		}
		if encoded == "" {
			missing = errors.Join(missing, fmt.Errorf("persistence encryption: %s is required", envMasterKey))
		}
		if missing != nil {
			return nil, missing
		}
	default:
		return nil, errors.New("persistence encryption: unsupported key provider")
	}
	key, err := base64.StdEncoding.DecodeString(encoded)
	if err != nil {
		return nil, errors.New("persistence encryption: key must be base64 encoded")
	}
	a, err := aes256GCM(key)
	if err != nil {
		return nil, err
	}
	return aesKeyProvider{id: id, cipher: a}, nil
}

func (p aesKeyProvider) Wrap(_ context.Context, key []byte) (WrappedKey, error) {
	return WrappedKey{KeyID: p.id, Ciphertext: p.cipher.Seal(nil, nil, key, []byte(p.id))}, nil
}
func (p aesKeyProvider) Unwrap(_ context.Context, w WrappedKey) ([]byte, error) {
	if w.KeyID != p.id {
		return nil, errors.New("persistence encryption: key ID does not match configuration")
	}
	return p.cipher.Open(nil, nil, w.Ciphertext, []byte(p.id))
}

type encryptedBackend struct {
	PersistenceBackend
	keys KeyProvider
}

// Keep the envelope format independent of the task's persisted state version.
type encryptedRecord struct {
	Version int        `json:"encryptionVersion"`
	Key     WrappedKey `json:"wrappedKey"`
	Data    []byte     `json:"ciphertext"`
}

func (b encryptedBackend) Save(ctx context.Context, recordKey string, data []byte) error {
	dek := make([]byte, 32)
	if _, err := rand.Read(dek); err != nil {
		return err
	}
	a, err := aes256GCM(dek)
	if err != nil {
		return err
	}
	wrapped, err := b.keys.Wrap(ctx, dek)
	if err != nil {
		return fmt.Errorf("persistence encryption: wrap data key: %w", err)
	}
	record, err := json.Marshal(encryptedRecord{Version: 1, Key: wrapped, Data: a.Seal(nil, nil, data, []byte(recordKey))})
	if err != nil {
		return err
	}
	return b.PersistenceBackend.Save(ctx, recordKey, record)
}

func (b encryptedBackend) Load(ctx context.Context, recordKey string) ([]byte, error) {
	raw, err := b.PersistenceBackend.Load(ctx, recordKey)
	if err != nil {
		return nil, err
	}
	var record encryptedRecord
	if err := json.Unmarshal(raw, &record); err != nil {
		return nil, fmt.Errorf("persistence encryption: invalid envelope: %w", err)
	}
	if record.Version != 1 {
		return nil, errors.New("persistence encryption: expected encrypted record version 1")
	}
	dek, err := b.keys.Unwrap(ctx, record.Key)
	if err != nil {
		return nil, fmt.Errorf("persistence encryption: unwrap data key: %w", err)
	}
	a, err := aes256GCM(dek)
	if err != nil {
		return nil, err
	}
	return a.Open(nil, nil, record.Data, []byte(recordKey))
}

func (b encryptedBackend) Check(ctx context.Context) error { return checkPersistenceBackend(ctx, b) }
