package main

import (
	"cloud.google.com/go/spanner"
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
	SpannerInstance string `usage:"Spanner instance, in the form of projects/{project}/instances/{instance}/databases/{database} or {project}/{instance}/{database}" required:"true"`
}

func (c *Cli) Run() error {
	// Set up Spanner client
	ctx := context.Background()
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
	client, err := spanner.NewClientWithConfig(ctx, c.SpannerInstance, spanner.ClientConfig{
		SessionPoolConfig:    spanner.DefaultSessionPoolConfig,
		SessionLabels:        map[string]string{"application_name": "spanner-console"},
		DisableRouteToLeader: false,
	})
	if err != nil {
		log.Fatalf("Failed to create Spanner client: %v", err)
	}

	defer client.Close()

	stat, _ := os.Stdin.Stat()
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
			err := Execute(ctx, client, string(line))
			if err != nil {
				return errors.WithStack(err)
			}
		}
		return nil
	}
	return Loop(prompt, func(query string) {
		err := Execute(ctx, client, query)
		if err != nil {
			fmt.Printf("Failed to execute query: %v\n", err)
		}
	})

}
