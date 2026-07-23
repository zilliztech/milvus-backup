package param

import (
	"fmt"
	"slices"
	"strings"
)

// List is a string list parameter. In a config file it is a YAML list; as an
// environment variable or --set override it is a comma-separated string, since
// those sources carry no structure.
type List struct {
	Default []string
	Keys    []string
	EnvKeys []string
	Opts    ValueOption

	Val  []string
	Used Used
}

func (l *List) Display(name string) Entry {
	value := strings.Join(l.Val, ",")
	if l.IsSecret() {
		value = maskSecret(value)
	}
	return Entry{
		Name:      name,
		Value:     value,
		Source:    l.Used.Kind,
		SourceKey: l.Used.Key,
	}
}

func (l *List) ConfigKeys() []string { return l.Keys }
func (l *List) EnvNames() []string   { return l.EnvKeys }
func (l *List) YAMLValue() any       { return l.Val }

func (l *List) UseDefault() {
	l.Val = slices.Clone(l.Default)
	l.Used = Used{Kind: SourceDefault}
}

func (l *List) IsDefault() bool { return l.Used.Kind == SourceDefault }

func (l *List) isRequired() bool { return l.Opts&RequiredValue != 0 }
func (l *List) IsSecret() bool   { return l.Opts&SecretValue != 0 }

// parseValue splits the comma-separated form used by env vars and overrides,
// dropping empty items so "a,b," and " a , b " both yield [a b].
func (l *List) parseValue(value string) []string {
	items := make([]string, 0, strings.Count(value, ",")+1)
	for item := range strings.SplitSeq(value, ",") {
		if item = strings.TrimSpace(item); item != "" {
			items = append(items, item)
		}
	}

	return items
}

// parseAny decodes the config file form. Only a YAML list is accepted: a
// scalar is almost always a v1 comma-separated string, which is worth an
// explicit error instead of a one-element list.
func (l *List) parseAny(key string, raw any) ([]string, error) {
	list, ok := raw.([]any)
	if !ok {
		return nil, fmt.Errorf("cfg: %s must be a YAML list, got %T", key, raw)
	}

	items := make([]string, 0, len(list))
	for _, item := range list {
		switch v := item.(type) {
		case string:
			if v = strings.TrimSpace(v); v != "" {
				items = append(items, v)
			}
		case nil:
			return nil, fmt.Errorf("cfg: %s contains an empty item", key)
		default:
			items = append(items, fmt.Sprint(v))
		}
	}

	return items, nil
}

func (l *List) Resolve(s *Source) error {
	// precedence: override > env > config file > default
	keys := make([]string, 0, len(l.Keys)+len(l.EnvKeys))
	keys = append(keys, l.Keys...)
	keys = append(keys, l.EnvKeys...)
	for _, key := range keys {
		if raw, ok := s.lookupOverride(key); ok {
			l.Val, l.Used = l.parseValue(raw), Used{Kind: SourceOverride, Key: strings.ToLower(key)}
			return nil
		}
	}

	for _, key := range l.EnvKeys {
		if raw, ok := s.lookupEnv(key); ok {
			l.Val, l.Used = l.parseValue(raw), Used{Kind: SourceEnv, Key: strings.ToLower(key)}
			return nil
		}
	}

	for _, key := range l.Keys {
		if raw, ok := s.lookupConfigFile(key); ok {
			items, err := l.parseAny(key, raw)
			if err != nil {
				return err
			}
			l.Val, l.Used = items, Used{Kind: SourceConfigFile, Key: strings.ToLower(key)}
			return nil
		}
	}

	if l.isRequired() {
		return fmt.Errorf("cfg: required value not found (keys=%v envKeys=%v)", l.Keys, l.EnvKeys)
	}

	l.Val, l.Used = slices.Clone(l.Default), Used{Kind: SourceDefault}

	return nil
}
