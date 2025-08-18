// Package main provides the entry point for the pg_flo PostgreSQL replication tool.
package main

import (
	"fmt"
	"os"

	"github.com/pgflo/pg_flo/cmd"
)

func main() {
	if err := cmd.Execute(); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}
