package integration

import (
	"context"
	"testing"

	"github.com/arori/job-scheduler/internal/store"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go/modules/mongodb"
	"go.mongodb.org/mongo-driver/mongo"
)

// setupMongo spins up a real single-node Mongo replica set in a container
// and returns a connected client plus a cleanup func. We test against a
// real Mongo instance rather than a mock because the whole point of
// ClaimDueJobs is atomicity guarantees that a mock can't meaningfully
// verify — a mock will happily let you "prove" a race-free implementation
// that isn't.
func setupMongo(t *testing.T) (*mongo.Database, func()) {
	t.Helper()
	ctx := context.Background()

	container, err := mongodb.Run(ctx, "mongo:7", mongodb.WithReplicaSet("rs0"))
	require.NoError(t, err, "failed to start mongo container")

	uri, err := container.ConnectionString(ctx)
	require.NoError(t, err)

	client, err := store.Connect(ctx, uri)
	require.NoError(t, err)

	db := client.Database("job_scheduler_test")

	cleanup := func() {
		_ = client.Disconnect(ctx)
		_ = container.Terminate(ctx)
	}
	return db, cleanup
}
