package main

import (
	"context"
	"encoding/csv"
	"fmt"
	"github.com/jedib0t/go-pretty/v6/table"
	"os"
)

// OutputFormat represents the format for query results
type OutputFormat string

const (
	// TableFormat represents the table output format
	TableFormat OutputFormat = "table"
	// CSVFormat represents the CSV output format
	CSVFormat OutputFormat = "csv"
)

// ResultWriter interface for writing query results
type ResultWriter interface {
	SetHeader(columns []string)
	AppendRow(row []interface{})
	Render()
}

// TableWriter implements ResultWriter using table format
type TableWriter struct {
	writer table.Writer
}

// NewTableWriter creates a new TableWriter
func NewTableWriter() ResultWriter {
	t := table.NewWriter()
	t.SetOutputMirror(os.Stdout)
	return &TableWriter{writer: t}
}

func (t *TableWriter) SetHeader(columns []string) {
	header := make(table.Row, len(columns))
	for i, col := range columns {
		header[i] = col
	}
	t.writer.AppendHeader(header)
}

func (t *TableWriter) AppendRow(row []interface{}) {
	tableRow := make(table.Row, len(row))
	for i, val := range row {
		tableRow[i] = val
	}
	t.writer.AppendRow(tableRow)
}

func (t *TableWriter) Render() {
	t.writer.Render()
}

// CSVWriter implements ResultWriter using CSV format
type CSVWriter struct {
	writer *csv.Writer
}

// NewCSVWriter creates a new CSVWriter
func NewCSVWriter() ResultWriter {
	return &CSVWriter{
		writer: csv.NewWriter(os.Stdout),
	}
}

func (c *CSVWriter) SetHeader(columns []string) {
	c.writer.Write(columns)
}

func (c *CSVWriter) AppendRow(row []interface{}) {
	strRow := make([]string, len(row))
	for i, val := range row {
		if val == nil {
			strRow[i] = ""
		} else {
			strRow[i] = stringify(val)
		}
	}
	c.writer.Write(strRow)
}

func (c *CSVWriter) Render() {
	c.writer.Flush()
}

// stringify converts any value to a string representation
func stringify(val interface{}) string {
	if val == nil {
		return ""
	}
	return fmt.Sprintf("%v", val)
}

// GetResultWriter returns the appropriate ResultWriter based on format
func GetResultWriter(format string) ResultWriter {
	if format == string(CSVFormat) {
		return NewCSVWriter()
	}
	return NewTableWriter()
}

// DatabaseClient defines the interface for database operations
type DatabaseClient interface {
	// Execute runs a query and returns the results
	Execute(ctx context.Context, query string) error

	ExecuteInTx(ctx context.Context, queries []string) error

	// Close releases any resources
	Close()

	// GetName returns a descriptive name for the connection
	GetName() string

	// ListTables lists all tables in the database
	ListTables(ctx context.Context) error
}
