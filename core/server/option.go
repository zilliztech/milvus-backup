package server

import "strings"

// Config for setting params used by server.
type config struct {
	port string
}

func newDefaultConfig() *config {
	return &config{
		port: ":8080",
	}
}

// Option is used to config the retry function.
type Option func(cfg *config)

func Port(port string) Option {
	return func(c *config) {
		if !strings.HasPrefix(port, ":") {
			port = ":" + port
		}
		c.port = port
	}
}
