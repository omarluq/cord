package playground

import (
	"net/url"
	"strings"
)

func compilerEndpoint(pageURL *url.URL, endpoint, configuredEndpoint string) (string, bool) {
	if endpoint == "" {
		endpoint = configuredEndpoint
	}

	parsed, ok := parseCompilerEndpoint(endpoint)
	if !ok {
		return "", false
	}

	resolved := pageURL.ResolveReference(parsed)
	if sameOrigin(pageURL, resolved) {
		return resolved.String(), true
	}

	if endpoint == configuredEndpoint && parsed.IsAbs() && parsed.Scheme == "https" {
		return parsed.String(), true
	}

	// Local development serves the static app and compiler on separate ports.
	if isLoopbackHost(pageURL.Hostname()) && endpoint == defaultCompilerURL {
		return endpoint, true
	}

	return "", false
}

func parseCompilerEndpoint(endpoint string) (*url.URL, bool) {
	if endpoint == "" {
		return nil, false
	}

	parsed, err := url.Parse(endpoint)

	return parsed, err == nil && parsed.User == nil
}

func sameOrigin(first, second *url.URL) bool {
	return first.Scheme == second.Scheme && first.Host == second.Host
}

func isLoopbackHost(host string) bool {
	return host == "localhost" || host == "127.0.0.1" || host == "::1" ||
		strings.HasSuffix(host, ".localhost")
}
