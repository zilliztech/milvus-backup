package v2

import (
	"bytes"
	"fmt"
	"strings"

	"gopkg.in/yaml.v3"

	"github.com/zilliztech/milvus-backup/internal/cfg/param"
)

// Render serializes a resolved configuration back into a complete v2 YAML file:
// the configVersion header followed by every parameter written with its
// resolved value. Secrets are written in the clear, since the output is a
// working config file rather than a display.
//
// comments attaches a head comment to the parameter named by its lower-cased
// config key (for example "milvus.storage.auth.secretAccessKey"); a nil map
// writes no comments. Keys are emitted in schema declaration order, so the
// output reads like the hand-written sample.
//
// Render is the inverse of Load over resolved values: loading the output yields
// a configuration whose values equal the input's.
func Render(c *Config, comments map[string]string) ([]byte, error) {
	skip := skippableKeys(c)

	root := &yaml.Node{Kind: yaml.MappingNode}
	insert(root, []string{VersionKey}, Version, "")

	param.Walk(c, func(_ string, f param.Field) {
		keys := f.ConfigKeys()
		if len(keys) == 0 {
			return
		}
		key := strings.ToLower(keys[0])
		if _, ok := skip[key]; ok {
			return
		}
		insert(root, strings.Split(keys[0], "."), f.YAMLValue(), comments[key])
	})

	var buf bytes.Buffer
	enc := yaml.NewEncoder(&buf)
	enc.SetIndent(2)
	if err := enc.Encode(root); err != nil {
		return nil, fmt.Errorf("cfg: render v2 config: %w", err)
	}
	if err := enc.Close(); err != nil {
		return nil, fmt.Errorf("cfg: render v2 config: %w", err)
	}

	return buf.Bytes(), nil
}

// skippableKeys returns the lower-cased config keys Render must omit because
// the v2 loader would reject them: storage credential fields that do not apply
// to the selected provider or authentication type. Emitting them, even empty,
// would fail validation on reload, since a key present in a file resolves as
// explicitly set rather than defaulted.
func skippableKeys(c *Config) map[string]struct{} {
	skip := map[string]struct{}{}
	for _, s := range []*StorageConfig{&c.Milvus.Storage, &c.Backup.Storage} {
		for _, key := range s.inapplicableKeys() {
			skip[strings.ToLower(key)] = struct{}{}
		}
	}

	return skip
}

// inapplicableKeys lists the config keys of this storage config's identity and
// credential fields that do not apply to its provider and authentication type,
// mirroring the compatibility rules validate.go enforces.
func (c *StorageConfig) inapplicableKeys() []string {
	applies := map[string]bool{}
	if c.Provider.Val != ProviderLocal {
		switch c.Auth.Type.Val {
		case AuthStatic:
			markKeys(applies, &c.Auth.AccessKeyID, &c.Auth.SecretAccessKey, &c.Auth.SessionToken)
		case AuthSharedKey:
			markKeys(applies, &c.Auth.AccountKey)
		case AuthServiceAccount:
			markKeys(applies, &c.Auth.CredentialsFile)
		case AuthIAM:
			markKeys(applies, &c.Auth.Endpoint)
		}
	}

	var skip []string
	// accountName belongs to the Azure storage identity and nothing else.
	if c.Provider.Val != ProviderAzure {
		skip = append(skip, c.AccountName.Keys...)
	}
	for _, cred := range []*param.Value[string]{
		&c.Auth.AccessKeyID, &c.Auth.SecretAccessKey, &c.Auth.SessionToken,
		&c.Auth.AccountKey, &c.Auth.CredentialsFile, &c.Auth.Endpoint,
	} {
		if len(cred.Keys) > 0 && !applies[cred.Keys[0]] {
			skip = append(skip, cred.Keys[0])
		}
	}

	return skip
}

func markKeys(applies map[string]bool, fields ...*param.Value[string]) {
	for _, f := range fields {
		if len(f.Keys) > 0 {
			applies[f.Keys[0]] = true
		}
	}
}

// insert writes value at the dotted path under root, creating intermediate
// mapping nodes as needed and preserving the order keys are first seen in.
func insert(root *yaml.Node, path []string, value any, comment string) {
	m := root
	for _, key := range path[:len(path)-1] {
		m = child(m, key)
	}

	valNode := &yaml.Node{}
	if err := valNode.Encode(value); err != nil {
		// Encoding a scalar or []string never fails in practice; fall back to a
		// string so a value can never silently vanish from the output.
		valNode.SetString(fmt.Sprint(value))
	}

	keyNode := &yaml.Node{Kind: yaml.ScalarNode, Value: path[len(path)-1]}
	if comment != "" {
		keyNode.HeadComment = comment
	}
	m.Content = append(m.Content, keyNode, valNode)
}

// child returns the mapping node stored under key in m, creating an empty one
// if it does not exist yet.
func child(m *yaml.Node, key string) *yaml.Node {
	for i := 0; i+1 < len(m.Content); i += 2 {
		if m.Content[i].Value == key {
			return m.Content[i+1]
		}
	}

	keyNode := &yaml.Node{Kind: yaml.ScalarNode, Value: key}
	valNode := &yaml.Node{Kind: yaml.MappingNode}
	m.Content = append(m.Content, keyNode, valNode)

	return valNode
}
