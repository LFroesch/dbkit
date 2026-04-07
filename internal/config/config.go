package config

import (
	"encoding/json"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
)

// Connection represents a saved database connection.
type Connection struct {
	ID   string `json:"id"`
	Name string `json:"name"`
	Type string `json:"type"` // sqlite, postgres, mongo
	DSN  string `json:"dsn"`
}

// Config holds all persisted state.
type Config struct {
	Connections []Connection `json:"connections"`
}

func configPath() string {
	home, _ := os.UserHomeDir()
	return filepath.Join(home, ".dbkit", "config.json")
}

// Load reads config from disk. Returns empty config if not found.
func Load() (*Config, error) {
	data, err := os.ReadFile(configPath())
	if err != nil {
		if os.IsNotExist(err) {
			return &Config{}, nil
		}
		return nil, err
	}
	var cfg Config
	if err := json.Unmarshal(data, &cfg); err != nil {
		return nil, err
	}
	return &cfg, nil
}

// Save writes config to disk.
func (c *Config) Save() error {
	path := configPath()
	if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
		return err
	}
	data, err := json.MarshalIndent(c, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(path, data, 0644)
}

// AddConnection appends a new connection and saves.
func (c *Config) AddConnection(name, dbType, dsn string) Connection {
	conn := Connection{
		ID:   fmt.Sprintf("%08x", rand.Uint32()),
		Name: name,
		Type: dbType,
		DSN:  dsn,
	}
	c.Connections = append(c.Connections, conn)
	return conn
}

// DeleteConnection removes a connection by index and saves.
func (c *Config) DeleteConnection(idx int) {
	if idx < 0 || idx >= len(c.Connections) {
		return
	}
	c.Connections = append(c.Connections[:idx], c.Connections[idx+1:]...)
}
