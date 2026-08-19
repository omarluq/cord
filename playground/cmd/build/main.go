// Command build assembles the Cord playground as a static website.
package main

import (
	"errors"
	"flag"
	"fmt"
	"net/url"
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

	if err := validateCompilerURL(os.Getenv("CORD_COMPILER_URL")); err != nil {
		return err
	}

	if err := playground.GenerateStatic(*output, *prefix); err != nil {
		return fmt.Errorf("generate playground: %w", err)
	}

	return nil
}

func validateCompilerURL(value string) error {
	if value == "" {
		return nil
	}

	compilerURL, err := url.Parse(value)
	if err != nil || compilerURL.Scheme != "https" || compilerURL.Host == "" ||
		compilerURL.User != nil || compilerURL.Path != "/compile" ||
		compilerURL.RawQuery != "" || compilerURL.Fragment != "" {
		return errors.New("CORD_COMPILER_URL must be an HTTPS URL ending in /compile")
	}

	return nil
}
