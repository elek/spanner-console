package main

import (
	"context"
	"cloud.google.com/go/bigquery"
	"encoding/hex"
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
	schema := it.Schema
	var header table.Row
	for _, field := range schema {
		header = append(header, field.Name)
	}
	t.AppendHeader(header)
	
	// Print rows
	for {
		var row []bigquery.Value
		err := it.Next(&row)
		if err == iterator.Done {
			break
		}
		if err != nil {
			return err
		}
		
		var tableRow table.Row
		for i, val := range row {
			tableRow = append(tableRow, formatBigQueryValue(val, schema[i].Type))
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
