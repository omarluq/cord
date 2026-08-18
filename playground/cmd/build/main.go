// Command build assembles the Cord playground as a static website.
package main

import (
	"flag"
	"fmt"
	"os"

	"github.com/omarluq/cord/playground"
)

func main() {
	if err := run(os.Args[1:]); err != nil {
		if _, writeErr := fmt.Fprintln(os.Stderr, err); writeErr != nil {
			os.Exit(1)
		}
		os.Exit(1)
	}
}

func run(arguments []string) error {
	flags := flag.NewFlagSet("playground-build", flag.ContinueOnError)
	output := flags.String("output", "dist/playground", "static output directory")
	prefix := flags.String("prefix", "", "deployment URL prefix")
	if err := flags.Parse(arguments); err != nil {
		return fmt.Errorf("parse flags: %w", err)
	}

	if err := playground.GenerateStatic(*output, *prefix); err != nil {
		return fmt.Errorf("generate playground: %w", err)
	}

	return nil
}
