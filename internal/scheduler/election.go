package scheduler

import (
	"context"
	"fmt"
	"log"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/client/v3/concurrency"
)

const electionPrefix = "/job-scheduler/leader-election"

// LeaderElector wraps etcd's concurrency package to ensure exactly one
// scheduler instance runs the dispatch loop at a time. Standbys block in
// Campaign() until the current leader's session dies (crash, network
// partition, or clean shutdown), at which point etcd resolves a new
// leader via Raft — no split-brain window where two instances both
// believe they're leading.
type LeaderElector struct {
	client   *clientv3.Client
	session  *concurrency.Session
	election *concurrency.Election
	nodeID   string
	leaseTTL int
}

// NewLeaderElector connects to etcd and prepares (but does not yet start)
// this instance's participation in the election. leaseTTL controls how
// long a dead leader's claim survives before etcd lets someone else take
// over — shorter means faster failover, longer means more tolerance for
// transient network blips before a false failover is triggered.
func NewLeaderElector(endpoints []string, nodeID string, leaseTTL int) (*LeaderElector, error) {
	client, err := clientv3.New(clientv3.Config{
		Endpoints:   endpoints,
		DialTimeout: 5 * time.Second,
	})
	if err != nil {
		return nil, fmt.Errorf("etcd client init: %w", err)
	}

	session, err := concurrency.NewSession(client, concurrency.WithTTL(leaseTTL))
	if err != nil {
		client.Close()
		return nil, fmt.Errorf("etcd session init: %w", err)
	}

	election := concurrency.NewElection(session, electionPrefix)

	return &LeaderElector{
		client:   client,
		session:  session,
		election: election,
		nodeID:   nodeID,
		leaseTTL: leaseTTL,
	}, nil
}

// RunAsLeader blocks until this instance becomes leader (or ctx is
// cancelled), then invokes onLeading. If leadership is lost mid-run —
// session expired, etcd unreachable — the context passed to onLeading is
// cancelled so the scheduler loop stops promptly instead of continuing to
// dispatch jobs as a "leader" that etcd no longer recognizes as one.
func (le *LeaderElector) RunAsLeader(ctx context.Context, onLeading func(leaderCtx context.Context)) error {
	log.Printf("[%s] campaigning for scheduler leadership...", le.nodeID)

	if err := le.election.Campaign(ctx, le.nodeID); err != nil {
		return fmt.Errorf("campaign failed: %w", err)
	}
	log.Printf("[%s] elected leader", le.nodeID)

	leaderCtx, cancelLeading := context.WithCancel(ctx)
	defer cancelLeading()

	// Watch our own session — if it dies (etcd lost, lease expired because
	// this process was too slow/network-partitioned), stop leading
	// immediately rather than trusting our own belief that we're leader.
	go func() {
		select {
		case <-le.session.Done():
			log.Printf("[%s] leadership session ended — stepping down", le.nodeID)
			cancelLeading()
		case <-ctx.Done():
		}
	}()

	onLeading(leaderCtx)

	// Best-effort clean resignation on normal shutdown so the next
	// standby doesn't wait out a full lease TTL unnecessarily.
	resignCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := le.election.Resign(resignCtx); err != nil {
		log.Printf("[%s] resign failed (will expire via lease TTL instead): %v", le.nodeID, err)
	}

	return nil
}

func (le *LeaderElector) Close() error {
	if err := le.session.Close(); err != nil {
		le.client.Close()
		return err
	}
	return le.client.Close()
}
