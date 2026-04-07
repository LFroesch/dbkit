package db

import "fmt"

// MongoDB is a stub — full implementation coming soon.
type MongoDB struct {
	uri string
}

func (d *MongoDB) Type() string { return "mongo" }
func (d *MongoDB) DSN() string  { return d.uri }
func (d *MongoDB) Connect() error {
	return fmt.Errorf("MongoDB support coming soon — connection saved")
}
func (d *MongoDB) Close()                                         {}
func (d *MongoDB) Ping() error                                    { return fmt.Errorf("not connected") }
func (d *MongoDB) GetTables() ([]string, error)                   { return nil, fmt.Errorf("not connected") }
func (d *MongoDB) GetTableSchema(t string) (*TableSchema, error)  { return nil, fmt.Errorf("not connected") }
func (d *MongoDB) RunQuery(q string) (*QueryResult, error)        { return nil, fmt.Errorf("not connected") }
