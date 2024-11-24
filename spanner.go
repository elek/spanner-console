package main

import (
	"cloud.google.com/go/spanner"
	"cloud.google.com/go/spanner/apiv1/spannerpb"
	"context"
	"encoding/hex"
	"fmt"
	"github.com/jedib0t/go-pretty/v6/table"
	"os"
	"time"
)

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
