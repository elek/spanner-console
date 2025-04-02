package main

import (
	"cloud.google.com/go/spanner"
	"cloud.google.com/go/spanner/apiv1/spannerpb"
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"github.com/jedib0t/go-pretty/v6/table"
	"google.golang.org/api/iterator"
	"os"
	"time"
)

type SpannerClient struct {
	client *spanner.Client
	name   string
}

func NewSpannerClient(ctx context.Context, connectionString string, prompt string) (*SpannerClient, error) {
	client, err := spanner.NewClientWithConfig(ctx, connectionString, spanner.ClientConfig{
		SessionPoolConfig:    spanner.DefaultSessionPoolConfig,
		SessionLabels:        map[string]string{"application_name": "spanner-console"},
		DisableRouteToLeader: false,
	})
	if err != nil {
		return nil, err
	}

	return &SpannerClient{
		client: client,
		name:   prompt,
	}, nil
}

func (s *SpannerClient) Execute(ctx context.Context, query string) error {
	return Execute(ctx, s.client, query)
}

func (s *SpannerClient) Close() {
	s.client.Close()
}

func (s *SpannerClient) GetName() string {
	return s.name
}

func (s *SpannerClient) ListTables(ctx context.Context) error {
	t := table.NewWriter()
	t.SetOutputMirror(os.Stdout)

	// Set up header
	t.AppendHeader(table.Row{"Table Name"})

	// Query for all tables in the database
	stmt := spanner.Statement{
		SQL: `SELECT table_name 
		      FROM information_schema.tables 
		      WHERE table_catalog = '' AND table_schema = '' 
		      ORDER BY table_name`,
	}

	iter := s.client.Single().Query(ctx, stmt)
	defer iter.Stop()

	for {
		row, err := iter.Next()
		if errors.Is(err, iterator.Done) {
			break
		}
		if err != nil {
			return err
		}

		var tableName string
		if err := row.Columns(&tableName); err != nil {
			return err
		}

		t.AppendRow(table.Row{tableName})
	}

	t.Render()
	fmt.Println()
	return nil
}

func Execute(ctx context.Context, client *spanner.Client, query string) error {
	t := table.NewWriter()
	t.SetOutputMirror(os.Stdout)

	var headerPrinted bool
	_, err := client.ReadWriteTransaction(ctx, func(ctx context.Context, transaction *spanner.ReadWriteTransaction) error {
		err := transaction.Query(ctx, spanner.Statement{
			SQL: query,
		}).Do(func(r *spanner.Row) error {
			if !headerPrinted {
				var header table.Row
				for _, name := range r.ColumnNames() {
					header = append(header, name)
				}
				t.AppendHeader(header)
				headerPrinted = true
			}
			t.AppendRow(convertToRow(r))
			return nil
		})
		return err
	})

	t.Render()
	fmt.Println()
	return err
}

func convertToRow(r *spanner.Row) table.Row {
	var row table.Row

	for ix := range r.Size() {

		switch r.ColumnType(ix).Code {
		case spannerpb.TypeCode_BOOL:
			var v bool
			err := r.Column(ix, &v)
			if err != nil {
				row = append(row, err.Error())
			}
			row = append(row, v)
		case spannerpb.TypeCode_STRING:
			var v *string
			err := r.Column(ix, &v)
			if err != nil {
				row = append(row, err.Error())
			}
			if v == nil {
				row = append(row, nil)
			} else {
				row = append(row, *v)
			}

		case spannerpb.TypeCode_INT64:
			var v *int64
			err := r.Column(ix, &v)
			if err != nil {
				row = append(row, err.Error())
			}
			if v == nil {
				row = append(row, "nil")
			} else {
				row = append(row, *v)
			}

		case spannerpb.TypeCode_BYTES:
			var v []byte
			err := r.Column(ix, &v)
			if err != nil {
				row = append(row, err.Error())
			}
			row = append(row, hex.EncodeToString(v))
		case spannerpb.TypeCode_TIMESTAMP:
			var v *time.Time
			err := r.Column(ix, &v)
			if err != nil {
				row = append(row, err.Error())
			}
			if v == nil {
				row = append(row, "nil")
			} else {
				row = append(row, (*v).Format(time.RFC3339))
			}

		default:
			row = append(row, "Unknown type: "+r.ColumnType(ix).Code.String())
		}
	}
	return row
}
