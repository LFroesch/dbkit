package config

import (
	"os"
	"path/filepath"
	"testing"
)

func TestSaveUsesOwnerOnlyPermissions(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)

	cfg := &Config{
		Connections: []Connection{{ID: "abc123", Name: "main", Type: "sqlite", DSN: "test.db"}},
	}
	if err := cfg.Save(); err != nil {
		t.Fatalf("Save() error = %v", err)
	}

	info, err := os.Stat(filepath.Join(home, ".config", "bobdb", "config.json"))
	if err != nil {
		t.Fatalf("Stat() error = %v", err)
	}
	if got := info.Mode().Perm(); got != configFileMode {
		t.Fatalf("file mode = %o, want %o", got, configFileMode)
	}
}

func TestLoadPrefersLegacyWhenNewConfigIsEmpty(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)

	if err := os.MkdirAll(filepath.Join(home, ".config", "bobdb"), configDirMode); err != nil {
		t.Fatalf("MkdirAll new config dir: %v", err)
	}
	if err := os.MkdirAll(filepath.Join(home, ".bobdb"), configDirMode); err != nil {
		t.Fatalf("MkdirAll legacy config dir: %v", err)
	}

	if err := os.WriteFile(filepath.Join(home, ".config", "bobdb", "config.json"), []byte("{\"connections\":[]}"), configFileMode); err != nil {
		t.Fatalf("WriteFile new config: %v", err)
	}
	legacyData := []byte("{\"connections\":[{\"id\":\"p1\",\"name\":\"legacy\",\"type\":\"postgres\",\"dsn\":\"postgres://example\"}]}")
	if err := os.WriteFile(filepath.Join(home, ".bobdb", "config.json"), legacyData, configFileMode); err != nil {
		t.Fatalf("WriteFile legacy config: %v", err)
	}

	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	if len(cfg.Connections) != 1 {
		t.Fatalf("connections = %d, want 1", len(cfg.Connections))
	}
	if cfg.Connections[0].Name != "legacy" {
		t.Fatalf("connection name = %q, want legacy", cfg.Connections[0].Name)
	}
}
