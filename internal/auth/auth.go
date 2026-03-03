package auth

import (
	"context"
	"encoding/base64"
	"net/http"
	"strings"
)

type contextKey string

const entityKey contextKey = "entity"

func entityFromAPIKey(apiKey string) string {
	if _, after, ok := strings.Cut(apiKey, "-"); ok {
		apiKey = after
	}
	if len(apiKey) <= 40 {
		return "local"
	}
	name := strings.TrimLeft(apiKey[40:], "_")
	if len(name) > 128 {
		name = name[:128]
	}
	if name == "" {
		return "local"
	}
	return name
}

func extractAPIKey(header string) string {
	header = strings.TrimPrefix(header, "Bearer ")
	if strings.HasPrefix(header, "Basic ") {
		encoded := strings.TrimPrefix(header, "Basic ")
		decoded, err := base64.StdEncoding.DecodeString(encoded)
		if err != nil {
			return encoded
		}
		if _, after, ok := strings.Cut(string(decoded), ":"); ok {
			return after
		}
		return string(decoded)
	}
	return header
}

func Middleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		entity := "local"
		if header := r.Header.Get("Authorization"); header != "" {
			apiKey := extractAPIKey(header)
			entity = entityFromAPIKey(apiKey)
		}
		ctx := context.WithValue(r.Context(), entityKey, entity)
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}

func EntityFromContext(ctx context.Context) string {
	if v, ok := ctx.Value(entityKey).(string); ok {
		return v
	}
	return "local"
}
