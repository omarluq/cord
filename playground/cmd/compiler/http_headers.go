package main

import (
	"encoding/json"
	"log/slog"
	"math"
	"net/http"
	"strconv"
	"strings"

	"github.com/omarluq/cord/playground/internal/protocol"
)

const (
	contentTypeHeader = "Content-Type"
	gzipEncoding      = "gzip"
	identityEncoding  = "identity"
)

func (service *service) setHeaders(
	response http.ResponseWriter,
	origin string,
) {
	response.Header().Set("X-Content-Type-Options", "nosniff")
	response.Header().Set("X-Frame-Options", "DENY")
	response.Header().Set(
		"Content-Security-Policy",
		"default-src 'none'; frame-ancestors 'none'",
	)
	response.Header().Set("Cache-Control", "no-store")

	if _, allowed := service.allowedOrigins[origin]; allowed {
		response.Header().Set("Access-Control-Allow-Origin", origin)
		response.Header().Set("Access-Control-Allow-Methods", "POST, OPTIONS")
		response.Header().Set("Access-Control-Allow-Headers", "Content-Type")
		appendVary(response.Header(), "Origin")
	}
}

func appendVary(header http.Header, field string) {
	for value := range strings.SplitSeq(header.Get("Vary"), ",") {
		if strings.EqualFold(strings.TrimSpace(value), field) {
			return
		}
	}

	header.Add("Vary", field)
}

func negotiateEncoding(value string) (string, bool) {
	if strings.TrimSpace(value) == "" {
		return identityEncoding, true
	}

	qualities := encodingQualities(value)
	gzipQuality := qualityFor(qualities, gzipEncoding, 0)
	identityQuality := identityQuality(qualities)

	if gzipQuality > 0 && gzipQuality >= identityQuality {
		return gzipEncoding, true
	}

	if identityQuality > 0 {
		return identityEncoding, true
	}

	return "", false
}

func encodingQualities(value string) map[string]float64 {
	qualities := make(map[string]float64)

	for item := range strings.SplitSeq(value, ",") {
		coding, quality, valid := parseEncoding(strings.TrimSpace(item))
		current, present := qualities[coding]

		if valid && (!present || quality > current) {
			qualities[coding] = quality
		}
	}

	return qualities
}

func identityQuality(qualities map[string]float64) float64 {
	if quality, present := qualities[identityEncoding]; present {
		return quality
	}

	if wildcard, present := qualities["*"]; present && wildcard == 0 {
		return 0
	}

	return 1
}

func qualityFor(qualities map[string]float64, coding string, defaultQuality float64) float64 {
	if quality, present := qualities[coding]; present {
		return quality
	}

	if quality, present := qualities["*"]; present {
		return quality
	}

	return defaultQuality
}

func parseEncoding(value string) (coding string, quality float64, valid bool) {
	parts := strings.Split(value, ";")
	coding = strings.ToLower(strings.TrimSpace(parts[0]))

	if coding == "" {
		return "", 0, false
	}

	quality = 1

	for _, parameter := range parts[1:] {
		name, raw, found := strings.Cut(strings.TrimSpace(parameter), "=")
		if !found || !strings.EqualFold(strings.TrimSpace(name), "q") {
			return "", 0, false
		}

		parsed, err := strconv.ParseFloat(strings.TrimSpace(raw), 64)
		if err != nil || math.IsNaN(parsed) || parsed < 0 || parsed > 1 {
			return "", 0, false
		}

		quality = parsed
	}

	return coding, quality, true
}

func parseAllowedOrigins(value string) map[string]struct{} {
	origins := make(map[string]struct{})

	for origin := range strings.SplitSeq(value, ",") {
		origin = strings.TrimSpace(origin)
		if origin != "" {
			origins[origin] = struct{}{}
		}
	}

	return origins
}

func writeJSONError(response http.ResponseWriter, message string, status int) {
	response.Header().Set(contentTypeHeader, protocol.JSONMediaType)
	response.WriteHeader(status)

	if err := json.NewEncoder(response).Encode(
		protocol.ErrorResponse{Error: message},
	); err != nil {
		slog.Error("write JSON error response", "error", err)
	}
}

func allowedMethods(path string) string {
	if path == compilePath {
		return "POST, OPTIONS"
	}

	return "GET"
}
