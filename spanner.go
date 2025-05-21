package main

import (
	"cloud.google.com/go/spanner"
	"cloud.google.com/go/spanner/apiv1/spannerpb"
	"context"
	"encoding/hex"
	"github.com/pkg/errors"

	"fmt"
	"google.golang.org/api/iterator"
	"time"
)

type SpannerClient struct {
	client      *spanner.Client
	name        string
	transaction *spanner.ReadWriteTransaction
}

func (s *SpannerClient) ExecuteInTx(ctx context.Context, queries []string) error {
	return Execute(ctx, s.client, queries)
}

var _ DatabaseClient = (*SpannerClient)(nil)

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
	return Execute(ctx, s.client, []string{query})
}

func (s *SpannerClient) Close() {
	s.client.Close()
}

func (s *SpannerClient) GetName() string {
	return s.name
}

func (s *SpannerClient) ListTables(ctx context.Context) error {
	writer := GetResultWriter(outputFormat)

	// Set up header
	writer.SetHeader([]string{"Table Name"})

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

		writer.AppendRow([]interface{}{tableName})
	}

	writer.Render()
	fmt.Println()
	return nil
}

func Execute(ctx context.Context, client *spanner.Client, queries []string) error {
	writer := GetResultWriter(outputFormat)

	var headerPrinted bool
	_, err := client.ReadWriteTransaction(ctx, func(ctx context.Context, transaction *spanner.ReadWriteTransaction) error {
		for _, query := range queries {
			if query == "" {
				continue
			}
			err := transaction.Query(ctx, spanner.Statement{
				SQL: query,
			}).Do(func(r *spanner.Row) error {
				if !headerPrinted {
					var header []string
					for _, name := range r.ColumnNames() {
						header = append(header, name)
					}
					writer.SetHeader(header)
					headerPrinted = true
				}
				writer.AppendRow(convertToRow(r))
				return nil
			})
			if err != nil {
				return errors.WithStack(err)
			}
		}
		return nil
	})

	writer.Render()
	fmt.Println()
	return err
}

func convertToRow(r *spanner.Row) []interface{} {
	var row []interface{}

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
