package cfg

import (
	"fmt"
	"math"
	"strconv"
	"strings"
)

type ValueOption uint8

const (
	SecretValue ValueOption = 1 << iota
	RequiredValue
)

//go:generate stringer -type=SourceKind
type SourceKind uint8

const (
	SourceUnknown SourceKind = iota
	SourceOverride
	SourceEnv
	SourceConfigFile
	SourceDefault
)

type Used struct {
	Kind SourceKind
	Key  string
}

type Value[T any] struct {
	Default T
	Keys    []string
	EnvKeys []string
	Opts    ValueOption

	val  T
	Used Used
}

func (val *Value[T]) Value() T         { return val.val }
func (val *Value[T]) isRequired() bool { return val.Opts&RequiredValue != 0 }
func (val *Value[T]) IsSecret() bool   { return val.Opts&SecretValue != 0 }

// Set sets the resolved value directly (useful for tests or programmatic config).
func (val *Value[T]) Set(v T) {
	val.val = v
	val.Used = Used{Kind: SourceOverride, Key: ""}
}

func (val *Value[T]) parseValue(value string) (T, error) {
	var zero T

	switch any(zero).(type) {
	case int:
		parsed, err := strconv.Atoi(value)
		if err != nil {
			return zero, fmt.Errorf("cfg: parse int value %s: %w", value, err)
		}
		return any(parsed).(T), nil
	case int64:
		parsed, err := strconv.ParseInt(value, 10, 64)
		if err != nil {
			return zero, fmt.Errorf("cfg: parse int64 value %s: %w", value, err)
		}
		return any(parsed).(T), nil
	case int32:
		parsed, err := strconv.ParseInt(value, 10, 32)
		if err != nil {
			return zero, fmt.Errorf("cfg: parse int32 value %s: %w", value, err)
		}
		return any(int32(parsed)).(T), nil
	case uint64:
		parsed, err := strconv.ParseUint(value, 10, 64)
		if err != nil {
			return zero, fmt.Errorf("cfg: parse uint64 value %s: %w", value, err)
		}
		return any(parsed).(T), nil
	case uint32:
		parsed, err := strconv.ParseUint(value, 10, 32)
		if err != nil {
			return zero, fmt.Errorf("cfg: parse uint32 value %s: %w", value, err)
		}
		return any(uint32(parsed)).(T), nil
	case uint:
		parsed, err := strconv.ParseUint(value, 10, 0)
		if err != nil {
			return zero, fmt.Errorf("cfg: parse uint value %s: %w", value, err)
		}
		return any(uint(parsed)).(T), nil
	case string:
		return any(value).(T), nil
	case bool:
		parsed, err := strconv.ParseBool(value)
		if err != nil {
			return zero, fmt.Errorf("cfg: parse bool value %s: %w", value, err)
		}
		return any(parsed).(T), nil
	default:
		return zero, fmt.Errorf("cfg: parse value %q: unsupported type %T", value, zero)
	}
}

func (val *Value[T]) parseAny(raw any) (T, error) {
	var zero T
	if raw == nil {
		return zero, fmt.Errorf("cfg: parse nil value")
	}
	if typed, ok := raw.(T); ok {
		return typed, nil
	}

	// Best-effort coercion for YAML-decoded values (often int/int64/float64).
	switch any(zero).(type) {
	case string:
		return any(fmt.Sprint(raw)).(T), nil
	case bool:
		switch v := raw.(type) {
		case bool:
			return any(v).(T), nil
		case string:
			return val.parseValue(v)
		default:
			return zero, fmt.Errorf("cfg: parse bool value %v: unsupported type %T", raw, raw)
		}
	case int:
		i64, err := coerceInt64(raw)
		if err != nil {
			return zero, err
		}
		maxInt := int64(^uint(0) >> 1)
		minInt := -maxInt - 1
		if i64 < minInt || i64 > maxInt {
			return zero, fmt.Errorf("cfg: int overflow %d", i64)
		}
		return any(int(i64)).(T), nil
	case int32:
		i64, err := coerceInt64(raw)
		if err != nil {
			return zero, err
		}
		const maxInt32 = int64(1<<31 - 1)
		const minInt32 = int64(-1 << 31)
		if i64 < minInt32 || i64 > maxInt32 {
			return zero, fmt.Errorf("cfg: int32 overflow %d", i64)
		}
		return any(int32(i64)).(T), nil
	case int64:
		i64, err := coerceInt64(raw)
		if err != nil {
			return zero, err
		}
		return any(i64).(T), nil
	case uint:
		u64, err := coerceUint64(raw)
		if err != nil {
			return zero, err
		}
		maxUint := uint64(^uint(0))
		if u64 > maxUint {
			return zero, fmt.Errorf("cfg: uint overflow %d", u64)
		}
		return any(uint(u64)).(T), nil
	case uint32:
		u64, err := coerceUint64(raw)
		if err != nil {
			return zero, err
		}
		const maxUint32 = uint64(1<<32 - 1)
		if u64 > maxUint32 {
			return zero, fmt.Errorf("cfg: uint32 overflow %d", u64)
		}
		return any(uint32(u64)).(T), nil
	case uint64:
		u64, err := coerceUint64(raw)
		if err != nil {
			return zero, err
		}
		return any(u64).(T), nil
	default:
		return zero, fmt.Errorf("cfg: parse value %v: unsupported type %T", raw, raw)
	}
}

func coerceInt64(raw any) (int64, error) {
	const maxInt64 = int64(^uint64(0) >> 1)
	const minInt64 = -maxInt64 - 1
	switch v := raw.(type) {
	case int:
		return int64(v), nil
	case int32:
		return int64(v), nil
	case int64:
		return v, nil
	case uint:
		if uint64(v) > uint64(maxInt64) {
			return 0, fmt.Errorf("cfg: int64 overflow %d", v)
		}
		return int64(v), nil
	case uint32:
		return int64(v), nil
	case uint64:
		if v > uint64(maxInt64) {
			return 0, fmt.Errorf("cfg: int64 overflow %d", v)
		}
		return int64(v), nil
	case float64:
		if v != math.Trunc(v) {
			return 0, fmt.Errorf("cfg: want integer but got %v", v)
		}
		if v < float64(minInt64) || v > float64(maxInt64) {
			return 0, fmt.Errorf("cfg: int64 overflow %v", v)
		}
		return int64(v), nil
	case string:
		parsed, err := strconv.ParseInt(v, 10, 64)
		if err != nil {
			return 0, fmt.Errorf("cfg: parse int64 value %q: %w", v, err)
		}
		return parsed, nil
	default:
		return 0, fmt.Errorf("cfg: parse int64 value %v: unsupported type %T", raw, raw)
	}
}

func coerceUint64(raw any) (uint64, error) {
	switch v := raw.(type) {
	case uint:
		return uint64(v), nil
	case uint32:
		return uint64(v), nil
	case uint64:
		return v, nil
	case int:
		if v < 0 {
			return 0, fmt.Errorf("cfg: uint64 underflow %d", v)
		}
		return uint64(v), nil
	case int32:
		if v < 0 {
			return 0, fmt.Errorf("cfg: uint64 underflow %d", v)
		}
		return uint64(v), nil
	case int64:
		if v < 0 {
			return 0, fmt.Errorf("cfg: uint64 underflow %d", v)
		}
		return uint64(v), nil
	case float64:
		if v != math.Trunc(v) {
			return 0, fmt.Errorf("cfg: want integer but got %v", v)
		}
		if v < 0 || v > float64(^uint64(0)) {
			return 0, fmt.Errorf("cfg: uint64 overflow %v", v)
		}
		return uint64(v), nil
	case string:
		parsed, err := strconv.ParseUint(v, 10, 64)
		if err != nil {
			return 0, fmt.Errorf("cfg: parse uint64 value %q: %w", v, err)
		}
		return parsed, nil
	default:
		return 0, fmt.Errorf("cfg: parse uint64 value %v: unsupported type %T", raw, raw)
	}
}

func (val *Value[T]) resolveOverride(s *source) (bool, error) {
	keys := make([]string, 0, len(val.Keys)+len(val.EnvKeys))
	keys = append(keys, val.Keys...)
	keys = append(keys, val.EnvKeys...)
	for _, key := range keys {
		if raw, ok := s.lookupOverride(key); ok {
			parsed, err := val.parseValue(raw)
			if err != nil {
				return false, fmt.Errorf("cfg: parse override value %q for %s: %w", raw, key, err)
			}
			val.val = parsed
			val.Used = Used{Kind: SourceOverride, Key: strings.ToLower(key)}
			return true, nil
		}
	}

	return false, nil
}

func (val *Value[T]) resolveEnv(s *source) (bool, error) {
	for _, key := range val.EnvKeys {
		if raw, ok := s.lookupEnv(key); ok {
			parsed, err := val.parseValue(raw)
			if err != nil {
				return false, fmt.Errorf("cfg: parse env value %q for %s: %w", raw, key, err)
			}
			val.val = parsed
			val.Used = Used{Kind: SourceEnv, Key: strings.ToLower(key)}
			return true, nil
		}
	}

	return false, nil
}

func (val *Value[T]) resolveConfigFile(s *source) (bool, error) {
	for _, key := range val.Keys {
		if raw, ok := s.lookupConfigFile(key); ok {
			parsed, err := val.parseAny(raw)
			if err != nil {
				return false, fmt.Errorf("cfg: parse config file value %v for %s: %w", raw, key, err)
			}

			val.val = parsed
			val.Used = Used{Kind: SourceConfigFile, Key: strings.ToLower(key)}
			return true, nil
		}
	}

	return false, nil
}

func (val *Value[T]) Resolve(s *source) error {
	// precedence: override > env > config file > default
	ok, err := val.resolveOverride(s)
	if err != nil {
		return fmt.Errorf("cfg: resolve override (keys=%v envKeys=%v): %w", val.Keys, val.EnvKeys, err)
	}
	if ok {
		return nil
	}

	ok, err = val.resolveEnv(s)
	if err != nil {
		return fmt.Errorf("cfg: resolve env (keys=%v envKeys=%v): %w", val.Keys, val.EnvKeys, err)
	}
	if ok {
		return nil
	}

	ok, err = val.resolveConfigFile(s)
	if err != nil {
		return fmt.Errorf("cfg: resolve config file (keys=%v): %w", val.Keys, err)
	}
	if ok {
		return nil
	}

	if val.isRequired() {
		return fmt.Errorf("cfg: required value not found (keys=%v envKeys=%v)", val.Keys, val.EnvKeys)
	}

	val.val = val.Default
	val.Used = Used{Kind: SourceDefault, Key: ""}
	return nil
}
