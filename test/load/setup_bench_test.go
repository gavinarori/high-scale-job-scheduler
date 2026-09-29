package load

import (
	"context"
	"testing"

	"github.com/arori/job-scheduler/internal/store"
	"github.com/testcontainers/testcontainers-go/modules/mongodb"
	"go.mongodb.org/mongo-driver/mongo"
)

// setupMongo mirrors test/integration/setup_test.go but lives in its own
// package since Go benchmarks (`go test -bench`) and correctness tests are
// kept separate — you don't want a slow load run picked up incidentally by
// `go test ./...` in CI, and vice versa.
func setupMongo(b *testing.B) (*mongo.Database, func()) {
	b.Helper()
	ctx := context.Background()

	container, err := mongodb.Run(ctx, "mongo:7", mongodb.WithReplicaSet("rs0"))
	if err != nil {
		b.Fatalf("failed to start mongo container: %v", err)
	}

	uri, err := container.ConnectionString(ctx)
	if err != nil {
		b.Fatalf("failed to get connection string: %v", err)
	}

	client, err := store.Connect(ctx, uri)
	if err != nil {
		b.Fatalf("failed to connect: %v", err)
	}

	db := client.Database("job_scheduler_load")
	cleanup := func() {
		_ = client.Disconnect(ctx)
		_ = container.Terminate(ctx)
	}
	return db, cleanup
}
