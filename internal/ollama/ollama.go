package ollama

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"strings"
	"time"

	"bobdb/internal/config"
)

const defaultHost = "http://localhost:11434"
const defaultModel = "qwen2.5:7b"

type Client struct {
	host  string
	model string
}

func New() *Client {
	cfg, _ := config.Load()

	host := os.Getenv("BOBDB_OLLAMA_HOST")
	if host == "" {
		host = os.Getenv("OLLAMA_HOST")
	}
	if host == "" && cfg != nil {
		host = cfg.OllamaHost
	}
	if host == "" {
		host = defaultHost
	}
	if !strings.HasPrefix(host, "http://") && !strings.HasPrefix(host, "https://") {
		host = "http://" + host
	}

	model := os.Getenv("BOBDB_OLLAMA_MODEL")
	if model == "" && cfg != nil {
		model = cfg.OllamaModel
	}
	if model == "" {
		model = defaultModel
	}
	return &Client{host: host, model: model}
}

const systemPrompt = `You are a database query assistant. Generate a single query from a natural language description.

Output ONLY the raw query — no explanation, no markdown fences, no commentary. Never apologize or explain.

For SQL (sqlite/postgres): a single SQL statement.
  SELECT * FROM users WHERE email LIKE '%@gmail.com' LIMIT 50;

For MongoDB: standard shell syntax using db.collection.method(filter).
  db.users.find({"email":{"$regex":"@gmail.com"}})
  db.users.find({"status":"active"})
  db.users.findOne({"_id":{"$oid":"abc123"}})
  db.users.aggregate([{"$group":{"_id":"$role","count":{"$sum":1}}}])
  db.users.updateOne({"_id":{"$oid":"abc123"}},{"$set":{"name":"test"}})
  db.users.updateMany({"status":"pending"},{"$set":{"status":"archived"}})
  db.users.insertOne({"name":"example","active":true})
  db.users.deleteOne({"_id":{"$oid":"abc123"}})
  db.users.deleteMany({"active":false})
  db.users.countDocuments({"status":"active"})

Use the provided schema to pick the correct collection and field names.`

// GenerateQuery sends a natural language prompt to ollama and returns a raw query string.
func (c *Client) GenerateQuery(ctx context.Context, prompt, dbType, schemaContext string) (string, error) {
	userMsg := fmt.Sprintf("Database type: %s\n", dbType)
	if schemaContext != "" {
		userMsg += "Schema:\n" + schemaContext + "\n"
	}
	userMsg += "\nRequest: " + prompt

	body := map[string]any{
		"model":  c.model,
		"stream": false,
		"messages": []map[string]string{
			{"role": "system", "content": systemPrompt},
			{"role": "user", "content": userMsg},
		},
	}

	data, _ := json.Marshal(body)

	req, err := http.NewRequestWithContext(ctx, "POST", c.host+"/api/chat", bytes.NewReader(data))
	if err != nil {
		return "", err
	}
	req.Header.Set("Content-Type", "application/json")

	client := &http.Client{Timeout: 60 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return "", fmt.Errorf("ollama: %w", err)
	}
	defer resp.Body.Close()

	var chatResp struct {
		Message struct {
			Content string `json:"content"`
		} `json:"message"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&chatResp); err != nil {
		return "", fmt.Errorf("ollama decode: %w", err)
	}

	query := strings.TrimSpace(chatResp.Message.Content)
	// Strip accidental markdown fences
	query = strings.TrimPrefix(query, "```sql")
	query = strings.TrimPrefix(query, "```javascript")
	query = strings.TrimPrefix(query, "```")
	query = strings.TrimSuffix(query, "```")
	return strings.TrimSpace(query), nil
}
