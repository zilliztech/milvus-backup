package core

import "strings"

// BackupConfig for setting params used by backup context and server.
type BackupConfig struct {
	port string
}

func newDefaultBackupConfig() *BackupConfig {
	return &BackupConfig{
		port: ":8080",
	}
}

// BackupOption is used to config the retry function.
type BackupOption func(*BackupConfig)

func Port(port string) BackupOption {
	return func(c *BackupConfig) {
		if !strings.HasPrefix(port, ":") {
			port = ":" + port
		}
		c.port = port
	}
}
