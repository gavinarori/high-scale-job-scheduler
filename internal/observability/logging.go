package observability

import (
	"log/slog"
	"os"
)

// NewLogger returns a JSON structured logger tagged with the service name,
// so log aggregation can filter by service without parsing free-text
// prefixes. Every binary should call this once at startup and pass the
// logger down instead of using the standard `log` package for anything
// beyond quick local debugging.
func NewLogger(service string) *slog.Logger {
	handler := slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	})
	return slog.New(handler).With("service", service)
}
