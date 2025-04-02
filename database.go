package main

import (
	"context"
)

// DatabaseClient defines the interface for database operations
type DatabaseClient interface {
	// Execute runs a query and returns the results
	Execute(ctx context.Context, query string) error
	
	// Close releases any resources
	Close()
	
	// GetName returns a descriptive name for the connection
	GetName() string
	
	// ListTables lists all tables in the database
	ListTables(ctx context.Context) error
}
