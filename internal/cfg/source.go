package cfg

import (
	"os"
	"strings"
)

type source struct {
	override   map[string]string
	configFile map[string]any
}

func (s *source) lookupOverride(key string) (string, bool) {
	val, ok := s.override[strings.ToLower(key)]
	return val, ok
}

func (s *source) lookupEnv(key string) (string, bool) {
	val, ok := os.LookupEnv(key)
	return val, ok
}

func (s *source) lookupConfigFile(key string) (any, bool) {
	val, ok := s.configFile[strings.ToLower(key)]
	return val, ok
}
