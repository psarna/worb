package graphql

import (
	"context"
	"net/http"
)

type contextKey string

const baseURLKey contextKey = "baseURL"

func BaseURLMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		baseURL := "http://" + r.Host
		ctx := context.WithValue(r.Context(), baseURLKey, baseURL)
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}

func GetBaseURL(ctx context.Context) string {
	if v, ok := ctx.Value(baseURLKey).(string); ok {
		return v
	}
	return ""
}
