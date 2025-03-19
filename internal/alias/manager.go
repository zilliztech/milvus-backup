// Copyright (C) 2019-2020 Zilliz. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed under the License
// is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
// or implied. See the License for the specific language governing permissions and limitations under the License.

package alias

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

// AliasConfig represents the configuration for a cluster alias
type AliasConfig struct {
	Name           string `json:"name"`
	ConfigFilePath string `json:"config_file_path"`
}

// AliasManager manages cluster aliases
type AliasManager struct {
	configPath string
	aliases    map[string]AliasConfig
}

// NewAliasManager creates a new AliasManager
func NewAliasManager() (*AliasManager, error) {
	homeDir, err := os.UserHomeDir()
	if err != nil {
		return nil, err
	}

	configDir := filepath.Join(homeDir, ".milvus-backup")
	if err := os.MkdirAll(configDir, 0755); err != nil {
		return nil, err
	}

	configPath := filepath.Join(configDir, "aliases.json")
	manager := &AliasManager{
		configPath: configPath,
		aliases:    make(map[string]AliasConfig),
	}

	// Load existing aliases if file exists
	if _, err := os.Stat(configPath); err == nil {
		if err := manager.load(); err != nil {
			return nil, err
		}
	}

	return manager, nil
}

// load loads aliases from the config file
func (m *AliasManager) load() error {
	data, err := os.ReadFile(m.configPath)
	if err != nil {
		return err
	}

	return json.Unmarshal(data, &m.aliases)
}

// save saves aliases to the config file
func (m *AliasManager) save() error {
	data, err := json.MarshalIndent(m.aliases, "", "  ")
	if err != nil {
		return err
	}

	return os.WriteFile(m.configPath, data, 0644)
}

// SetAlias sets an alias for a cluster
func (m *AliasManager) SetAlias(name, configPath string) error {
	// Validate config file exists
	if _, err := os.Stat(configPath); err != nil {
		return fmt.Errorf("config file not found: %s", configPath)
	}

	// Make sure we have an absolute path
	absPath, err := filepath.Abs(configPath)
	if err != nil {
		return err
	}

	m.aliases[name] = AliasConfig{
		Name:           name,
		ConfigFilePath: absPath,
	}

	return m.save()
}

// GetAlias gets an alias configuration
func (m *AliasManager) GetAlias(name string) (AliasConfig, error) {
	if alias, ok := m.aliases[name]; ok {
		return alias, nil
	}
	return AliasConfig{}, fmt.Errorf("alias not found: %s", name)
}

// ListAliases lists all aliases
func (m *AliasManager) ListAliases() []AliasConfig {
	aliases := make([]AliasConfig, 0, len(m.aliases))
	for _, alias := range m.aliases {
		aliases = append(aliases, alias)
	}
	return aliases
}

// DeleteAlias deletes an alias
func (m *AliasManager) DeleteAlias(name string) error {
	if _, ok := m.aliases[name]; !ok {
		return fmt.Errorf("alias not found: %s", name)
	}

	delete(m.aliases, name)
	return m.save()
}

// ParseBackupName parses a backup name with optional alias prefix
// Returns the alias name, backup name, and error if any
func (m *AliasManager) ParseBackupName(name string) (string, string, error) {
	parts := strings.Split(name, "/")
	if len(parts) == 1 {
		// No alias specified, use default
		return "", name, nil
	} else if len(parts) == 2 {
		// Format: alias/backup_name
		aliasName := parts[0]
		backupName := parts[1]
		
		// Check if alias exists
		if _, err := m.GetAlias(aliasName); err != nil {
			return "", "", err
		}
		
		return aliasName, backupName, nil
	}
	
	return "", "", errors.New("invalid backup name format, expected: [alias/]backup_name")
}

// GetConfigPath returns the config file path for an alias
// If no alias is specified (empty string), returns an empty string
func (m *AliasManager) GetConfigPath(aliasName string) (string, error) {
	if aliasName == "" {
		return "", nil
	}
	
	alias, err := m.GetAlias(aliasName)
	if err != nil {
		return "", err
	}
	
	return alias.ConfigFilePath, nil
}
