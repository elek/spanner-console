package main

import (
	"cloud.google.com/go/bigquery"
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"github.com/jedib0t/go-pretty/v6/table"
	"google.golang.org/api/iterator"
	"os"
	"time"
)

type BigQueryClient struct {
	client *bigquery.Client
	name   string
}

func NewBigQueryClient(ctx context.Context, projectID string) (*BigQueryClient, error) {
	client, err := bigquery.NewClient(ctx, projectID)
	if err != nil {
		return nil, err
	}

	return &BigQueryClient{
		client: client,
		name:   projectID,
	}, nil
}

func (b *BigQueryClient) Execute(ctx context.Context, query string) error {
	t := table.NewWriter()
	t.SetOutputMirror(os.Stdout)

	q := b.client.Query(query)
	it, err := q.Read(ctx)
	if err != nil {
		return err
	}

	// Print headers
	var schema bigquery.Schema

	// Print rows
	for {
		var row []bigquery.Value
		err := it.Next(&row)
		if errors.Is(err, iterator.Done) {
			break
		}
		if err != nil {
			return err
		}

		if len(schema) == 0 {
			schema = it.Schema
			var header table.Row
			for _, field := range schema {
				header = append(header, field.Name)
			}
			t.AppendHeader(header)
		}

		var tableRow table.Row
		for i, val := range row {
			// Check if schema has enough elements to avoid index out of range
			if i < len(schema) {
				tableRow = append(tableRow, formatBigQueryValue(val, schema[i].Type))
			} else {
				// If schema doesn't have enough elements, just format as string
				tableRow = append(tableRow, fmt.Sprintf("%v", val))
			}
		}
		t.AppendRow(tableRow)
	}

	t.Render()
	fmt.Println()
	return nil
}

func formatBigQueryValue(val interface{}, fieldType bigquery.FieldType) interface{} {
	if val == nil {
		return "nil"
	}

	switch fieldType {
	case bigquery.StringFieldType:
		return val
	case bigquery.BytesFieldType:
		if bytes, ok := val.([]byte); ok {
			return hex.EncodeToString(bytes)
		}
	case bigquery.TimestampFieldType:
		if t, ok := val.(time.Time); ok {
			return t.Format(time.RFC3339)
		}
	}

	return val
}

func (b *BigQueryClient) Close() {
	b.client.Close()
}

func (b *BigQueryClient) GetName() string {
	return b.name
}

func (b *BigQueryClient) ListTables(ctx context.Context) error {
	t := table.NewWriter()
	t.SetOutputMirror(os.Stdout)

	// Set up header
	t.AppendHeader(table.Row{"Dataset", "Table Name"})

	// List all datasets
	datasets := b.client.Datasets(ctx)

	for {
		dataset, err := datasets.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			return err
		}

		// List all tables in the dataset
		tables := dataset.Tables(ctx)

		for {
			tbl, err := tables.Next()
			if err == iterator.Done {
				break
			}
			if err != nil {
				return err
			}

			t.AppendRow(table.Row{dataset.DatasetID, tbl.TableID})
		}
	}

	t.Render()
	fmt.Println()
	return nil
}
