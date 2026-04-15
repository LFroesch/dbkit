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

type SavedQuery struct {
	Label string `json:"label"`
	Query string `json:"query"`
}

// Config holds all persisted state.
type Config struct {
	Connections  []Connection            `json:"connections"`
	QueryHistory map[string][]string     `json:"query_history,omitempty"`
	SavedQueries map[string][]SavedQuery `json:"saved_queries,omitempty"`
	OllamaHost   string                  `json:"ollama_host,omitempty"`
	OllamaModel  string                  `json:"ollama_model,omitempty"`
}

const (
	configDirMode  = 0o700
	configFileMode = 0o600
)

func configPath() string {
	home, _ := os.UserHomeDir()
	return filepath.Join(home, ".config", "dbkit", "config.json")
}

func legacyConfigPath() string {
	home, _ := os.UserHomeDir()
	return filepath.Join(home, ".dbkit", "config.json")
}

// Load reads config from disk. Returns empty config if not found.
func Load() (*Config, error) {
	path := configPath()
	data, err := os.ReadFile(path)
	if err != nil {
		if !os.IsNotExist(err) {
			return nil, err
		}

		// Backward compatibility: read from pre-XDG location if present.
		legacyPath := legacyConfigPath()
		data, err = os.ReadFile(legacyPath)
		if err != nil {
			if os.IsNotExist(err) {
				return &Config{}, nil
			}
			return nil, err
		}
	}
	var cfg Config
	if err := json.Unmarshal(data, &cfg); err != nil {
		return nil, err
	}
	if cfg.IsEmpty() {
		legacyCfg, err := loadConfigFile(legacyConfigPath())
		if err == nil && !legacyCfg.IsEmpty() {
			return legacyCfg, nil
		}
		if err != nil && !os.IsNotExist(err) {
			return nil, err
		}
	}
	return &cfg, nil
}

// Save writes config to disk. Refuses to overwrite a non-empty config
// with an empty one to prevent accidental data loss.
func (c *Config) Save() error {
	path := configPath()
	if err := os.MkdirAll(filepath.Dir(path), configDirMode); err != nil {
		return err
	}
	// Guard: never overwrite a config that has connections with an empty one.
	if c.IsEmpty() {
		existing, err := loadConfigFile(path)
		if err == nil && !existing.IsEmpty() {
			return fmt.Errorf("refusing to overwrite non-empty config with empty config")
		}
	}
	data, err := json.MarshalIndent(c, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(path, data, configFileMode)
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

func (c *Config) UpdateConnection(idx int, name, dbType, dsn string) bool {
	if idx < 0 || idx >= len(c.Connections) {
		return false
	}
	c.Connections[idx].Name = name
	c.Connections[idx].Type = dbType
	c.Connections[idx].DSN = dsn
	return true
}

// DeleteConnection removes a connection by index and saves.
func (c *Config) DeleteConnection(idx int) {
	if idx < 0 || idx >= len(c.Connections) {
		return
	}
	if c.QueryHistory != nil {
		delete(c.QueryHistory, c.Connections[idx].ID)
	}
	if c.SavedQueries != nil {
		delete(c.SavedQueries, c.Connections[idx].ID)
	}
	c.Connections = append(c.Connections[:idx], c.Connections[idx+1:]...)
}

func (c *Config) QueriesForConnection(connID string) []string {
	if c.QueryHistory == nil {
		return nil
	}
	history := c.QueryHistory[connID]
	if len(history) == 0 {
		return nil
	}
	out := make([]string, len(history))
	copy(out, history)
	return out
}

func (c *Config) PushQuery(connID, query string, limit int) {
	if connID == "" || query == "" {
		return
	}
	if limit <= 0 {
		limit = 50
	}
	if c.QueryHistory == nil {
		c.QueryHistory = map[string][]string{}
	}
	history := c.QueryHistory[connID]
	filtered := make([]string, 0, len(history))
	for _, existing := range history {
		if existing == query {
			continue
		}
		filtered = append(filtered, existing)
	}
	history = append([]string{query}, filtered...)
	if len(history) > limit {
		history = history[:limit]
	}
	c.QueryHistory[connID] = history
}

func (c *Config) SavedQueriesForConnection(connID string) []SavedQuery {
	if c.SavedQueries == nil {
		return nil
	}
	saved := c.SavedQueries[connID]
	if len(saved) == 0 {
		return nil
	}
	out := make([]SavedQuery, len(saved))
	copy(out, saved)
	return out
}

func (c *Config) SaveQuery(connID, label, query string, limit int) {
	if connID == "" || query == "" {
		return
	}
	if limit <= 0 {
		limit = 50
	}
	if c.SavedQueries == nil {
		c.SavedQueries = map[string][]SavedQuery{}
	}
	saved := c.SavedQueries[connID]
	filtered := make([]SavedQuery, 0, len(saved))
	for _, existing := range saved {
		if existing.Query == query {
			continue
		}
		filtered = append(filtered, existing)
	}
	saved = append([]SavedQuery{{Label: label, Query: query}}, filtered...)
	if len(saved) > limit {
		saved = saved[:limit]
	}
	c.SavedQueries[connID] = saved
}

func (c *Config) IsEmpty() bool {
	return len(c.Connections) == 0 && len(c.QueryHistory) == 0 && len(c.SavedQueries) == 0
}

func loadConfigFile(path string) (*Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var cfg Config
	if err := json.Unmarshal(data, &cfg); err != nil {
		return nil, err
	}
	return &cfg, nil
}
