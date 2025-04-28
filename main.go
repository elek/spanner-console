package main

import (
	"context"
	"fmt"
	"github.com/alecthomas/kong"
	"github.com/pkg/errors"
	"io"
	"log"
	"os"
	"strings"
)

func main() {
	ktx := kong.Parse(&Cli{})
	err := ktx.Run()
	if err != nil {
		log.Fatalf("Failed to run: %v", err)
	}
}

type Cli struct {
	SpannerInstance string `name:"spanner" help:"Spanner instance, in the form of projects/{project}/instances/{instance}/databases/{database} or {project}/{instance}/{database}"`
	BigQueryProject string `name:"bigquery" help:"BigQuery project ID"`
	Transaction     bool   `name:"transaction" short:"t" help:"Execute all queries in a single transaction"`
}

func (c *Cli) Run() error {
	// Set up appropriate client based on which flag was provided
	ctx := context.Background()

	var dbClient DatabaseClient
	var err error

	if c.SpannerInstance != "" && c.BigQueryProject != "" {
		return errors.New("Cannot specify both --spanner and --bigquery")
	}

	if c.SpannerInstance != "" {
		// Handle Spanner connection string formatting
		parts := strings.Split(c.SpannerInstance, "/")
		if len(parts) != 6 && len(parts) != 3 {
			return errors.New(fmt.Sprintf("Invalid Spanner instance definition: %s", c.SpannerInstance))
		}
		prompt := c.SpannerInstance
		if len(parts) == 3 {
			c.SpannerInstance = fmt.Sprintf("projects/%s/instances/%s/databases/%s", parts[0], parts[1], parts[2])
		} else if len(parts) == 6 {
			prompt = fmt.Sprintf("%s/%s/%s", parts[1], parts[3], parts[5])
		} else {
			return errors.New(fmt.Sprintf("Invalid Spanner instance: %s", c.SpannerInstance))
		}

		dbClient, err = NewSpannerClient(ctx, c.SpannerInstance, prompt)
	} else if c.BigQueryProject != "" {
		dbClient, err = NewBigQueryClient(ctx, c.BigQueryProject)
	} else {
		return errors.New("Either --spanner or --bigquery must be specified")
	}

	if err != nil {
		log.Fatalf("Failed to create database client: %v", err)
	}

	defer dbClient.Close()

	stat, _ := os.Stdin.Stat()

	var queries []string
	if (stat.Mode() & os.ModeCharDevice) == 0 {
		content, err := io.ReadAll(os.Stdin)
		if err != nil {
			return errors.Wrap(err, "failed to read from stdin")
		}
		for _, line := range strings.Split(string(content), ";") {
			line = strings.TrimSpace(line)
			if line == "" {
				continue
			}
			fmt.Println(line)
			if c.Transaction {
				queries = append(queries, line)
			} else {
				err := dbClient.Execute(ctx, string(line))
				if err != nil {
					return errors.WithStack(err)
				}
			}
		}
		if len(queries) > 0 {
			err := dbClient.ExecuteInTx(ctx, queries)
			if err != nil {
				return errors.WithStack(err)
			}
		}
		return nil
	}

	return Loop(dbClient.GetName(), func(query string) {
		err := dbClient.Execute(ctx, query)
		if err != nil {
			fmt.Printf("Failed to execute query: %v\n", err)
		}
	}, dbClient)
}
