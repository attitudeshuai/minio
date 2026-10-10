// Copyright (c) 2024 MinIO, Inc.
//
// This file is part of MinIO Object Storage stack
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Affero General Public License for more details.
//
// You should have received a copy of the GNU Affero General Public License
// along with this program.  If not, see <http://www.gnu.org/licenses/>.

package cmd

import (
	"context"

	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/minio/minio/internal/admission"
	"github.com/minio/minio/internal/grid"
)

const (
	admissionExchangeIntervalEnv = "MINIO_ADMISSION_EXCHANGE_INTERVAL"
	admissionDefaultExchange     = time.Second
	admissionReloadPollInterval  = 30 * time.Second
)

var (
	admissionPeerSnapPool = grid.NewJSONPool[admission.PeerSnap]()
	admissionAllocPool    = grid.NewJSONPool[admission.Allocation]()
	admissionMutationPool = grid.NewJSONPool[admission.Mutation]()

	admissionExchangeRPC = grid.NewSingleHandler[*grid.JSON[admission.PeerSnap], grid.NoPayload](
		grid.HandlerAdmissionExchange, admissionPeerSnapPool.NewJSON, grid.NewNoPayload,
	).IgnoreNilConn()
	admissionAllocateRPC = grid.NewSingleHandler[*grid.JSON[admission.Allocation], grid.NoPayload](
		grid.HandlerAdmissionAllocate, admissionAllocPool.NewJSON, grid.NewNoPayload,
	).IgnoreNilConn()
	admissionMutateRPC = grid.NewSingleHandler[*grid.JSON[admission.Mutation], grid.NoPayload](
		grid.HandlerAdmissionMutate, admissionMutationPool.NewJSON, grid.NewNoPayload,
	).IgnoreNilConn()
	admissionReloadRPC = grid.NewSingleHandler[*grid.MSS, grid.NoPayload](
		grid.HandlerAdmissionReload, grid.NewMSS, grid.NewNoPayload,
	).IgnoreNilConn()

	// pushFailures counts failed snapshot/allocation pushes.
	admissionPushFailures atomic.Int64
)

const (
	// Phase waits inside the 1s exchange window: collect usage at the leader,
	// then deliver the allocation vector to followers. Both are skipped for
	// a single node.
	admissionCollectWait = 200 * time.Millisecond
	admissionAllocWait   = 200 * time.Millisecond
)

// AdmissionExchangeHandler receives one peer's per-window usage snapshot.
func (s *peerRESTServer) AdmissionExchangeHandler(snap *grid.JSON[admission.PeerSnap]) (grid.NoPayload, *grid.RemoteErr) {
	if snap == nil {
		return grid.NoPayload{}, nil
	}
	if err := globalAdmissionSys.mergeSnapshot(*snap.Value()); err != nil {
		return grid.NoPayload{}, grid.NewRemoteErr(err)
	}
	return grid.NoPayload{}, nil
}

// AdmissionAllocateHandler receives the leader's authoritative per-epoch
// rate vector.
func (s *peerRESTServer) AdmissionAllocateHandler(al *grid.JSON[admission.Allocation]) (grid.NoPayload, *grid.RemoteErr) {
	if al == nil {
		return grid.NoPayload{}, nil
	}
	if err := globalAdmissionSys.mergeAllocation(*al.Value()); err != nil {
		return grid.NoPayload{}, grid.NewRemoteErr(err)
	}
	return grid.NoPayload{}, nil
}

// AdmissionMutateHandler applies a forwarded rule mutation on the leader,
// the single writer of the shared rules file.
func (s *peerRESTServer) AdmissionMutateHandler(m *grid.JSON[admission.Mutation]) (grid.NoPayload, *grid.RemoteErr) {
	if m == nil {
		return grid.NoPayload{}, grid.NewRemoteErrString("missing mutation")
	}
	if err := globalAdmissionSys.applyLeaderMutation(GlobalContext, *m.Value()); err != nil {
		return grid.NoPayload{}, grid.NewRemoteErr(err)
	}
	return grid.NoPayload{}, nil
}

// AdmissionReloadHandler reloads rules from the shared metadata store.
func (s *peerRESTServer) AdmissionReloadHandler(_ *grid.MSS) (grid.NoPayload, *grid.RemoteErr) {
	objAPI := newObjectLayerFn()
	if objAPI == nil {
		return grid.NoPayload{}, grid.NewRemoteErrString("object layer not initialized")
	}
	if err := globalAdmissionSys.reloadFromStore(GlobalContext, objAPI); err != nil {
		internalLogIf(GlobalContext, err)
		// Fail open: keep serving with the previous rule set.
	}
	return grid.NoPayload{}, nil
}

// PushAdmissionSnapshot pushes one local usage snapshot to a peer.
func (client *peerRESTClient) PushAdmissionSnapshot(ctx context.Context, snap admission.PeerSnap) error {
	_, err := admissionExchangeRPC.Call(ctx, client.gridConn(), admissionPeerSnapPool.NewJSONWith(&snap))
	return err
}

// PushAdmissionAllocation pushes the leader allocation vector to a follower.
func (client *peerRESTClient) PushAdmissionAllocation(ctx context.Context, al admission.Allocation) error {
	_, err := admissionAllocateRPC.Call(ctx, client.gridConn(), admissionAllocPool.NewJSONWith(&al))
	return err
}

// ReloadAdmissionRules asks a peer to reload admission rules from store.
func (client *peerRESTClient) ReloadAdmissionRules(ctx context.Context) error {
	_, err := admissionReloadRPC.Call(ctx, client.gridConn(), grid.NewMSS())
	return err
}

// MutateAdmissionRules forwards an admin rule mutation to the leader node.
func (client *peerRESTClient) MutateAdmissionRules(ctx context.Context, m admission.Mutation) error {
	_, err := admissionMutateRPC.Call(ctx, client.gridConn(), admissionMutationPool.NewJSONWith(&m))
	return err
}

// ReloadAdmissionRules broadcasts a rules reload to all peers.
func (sys *NotificationSys) ReloadAdmissionRules(ctx context.Context) []NotificationPeerErr {
	ng := WithNPeers(len(sys.peerClients)).WithRetries(1)
	for idx, client := range sys.peerClients {
		ng.Go(ctx, func() error {
			if client == nil {
				return errPeerNotReachable
			}
			return client.ReloadAdmissionRules(ctx)
		}, idx, *client.host)
	}
	return ng.Wait()
}

// broadcastAdmissionReload is the post-mutation hook used by the store layer.
func broadcastAdmissionReload() {
	if globalNotificationSys == nil {
		return
	}
	// Fire and forget; the 30s store poll is the durable fallback.
	go func() {
		ctx, cancel := context.WithTimeout(GlobalContext, 10*time.Second)
		defer cancel()
		globalNotificationSys.ReloadAdmissionRules(ctx)
	}()
}

// admissionExchangeInterval reads the optional env override.
func admissionExchangeInterval() time.Duration {
	v := os.Getenv(admissionExchangeIntervalEnv)
	if v == "" {
		return admissionDefaultExchange
	}
	d, err := time.ParseDuration(v)
	if err != nil || d <= 0 {
		return admissionDefaultExchange
	}
	return d
}

// startAdmissionSystem loads persisted rules and wires lifecycle. The
// exchange/reload/reaper goroutines exist only while at least one rule is
// active; with no rules configured there are no background loops and no peer
// RPCs from this feature.
func startAdmissionSystem(ctx context.Context) {
	objAPI := newObjectLayerFn()
	if objAPI == nil {
		return
	}

	globalAdmissionSys.setStore(objAPI)
	globalAdmissionSys.initFromStore(ctx, objAPI)
	admissionPeersReloadHook = broadcastAdmissionReload
	if globalAdmissionSys.mgr.Enabled() {
		startAdmissionWorkers()
	}
}

var (
	admissionWorkerMu     sync.Mutex
	admissionWorkerCancel context.CancelFunc
)

// startAdmissionWorkers spawns the exchange/reload/reaper loops. It is a
// no-op when they are already running (including the single-node case).
func startAdmissionWorkers() {
	store := globalAdmissionSys.backingStore()
	if store == nil {
		return
	}
	admissionWorkerMu.Lock()
	defer admissionWorkerMu.Unlock()
	if admissionWorkerCancel != nil {
		return
	}
	ctx, cancel := context.WithCancel(GlobalContext)
	admissionWorkerCancel = cancel

	interval := admissionExchangeInterval()
	agg := admission.NewAggregator(globalAdmissionSys.mgr, globalLocalNodeName, interval, nil)
	globalAdmissionSys.setAggregator(agg)

	go runAdmissionExchangeLoop(ctx, agg, interval)
	go runAdmissionReloadPoller(ctx, store)
	go runAdmissionReaper(ctx, store)
}

// stopAdmissionWorkers stops all feature goroutines and the cluster aggregator.
func stopAdmissionWorkers() {
	admissionWorkerMu.Lock()
	defer admissionWorkerMu.Unlock()
	if admissionWorkerCancel != nil {
		admissionWorkerCancel()
		admissionWorkerCancel = nil
	}
	globalAdmissionSys.setAggregator(nil)
}

func runAdmissionExchangeLoop(ctx context.Context, agg *admission.Aggregator, interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			admissionExchangeTick(ctx, agg)
		}
	}
}

func admissionExchangeTick(ctx context.Context, agg *admission.Aggregator) {
	// With no rules there are no peer RPCs at all (AC zero-config).
	if !agg.Enabled() {
		return
	}

	// Phase 0: drain window counters and publish the local usage snapshot.
	snap := agg.BeginWindow()

	cluster := totalNodeCount() > 1
	if globalNotificationSys != nil && cluster {
		for _, client := range globalNotificationSys.peerClients {
			if client == nil {
				admissionPushFailures.Add(1)
				continue
			}
			if err := client.PushAdmissionSnapshot(ctx, snap); err != nil {
				admissionPushFailures.Add(1)
			}
		}
	}

	if !cluster {
		agg.LeaderReallocate()
		return
	}

	// Phase 1: give the leader time to collect peers' window usage.
	select {
	case <-ctx.Done():
		return
	case <-time.After(admissionCollectWait):
	}

	if agg.IsLeader() {
		// Leader computes the authoritative vector and pushes it before the
		// barrier so followers apply the same vector in this same window.
		alloc := agg.BuildAllocation()
		for _, client := range globalNotificationSys.peerClients {
			if client == nil {
				admissionPushFailures.Add(1)
				continue
			}
			if err := client.PushAdmissionAllocation(ctx, alloc); err != nil {
				admissionPushFailures.Add(1)
			}
		}
		// Apply at the barrier together with the followers.
		select {
		case <-ctx.Done():
			return
		case <-time.After(admissionAllocWait):
		}
		agg.ApplyEpoch(alloc.Epoch)
		return
	}

	// Phase 2 (followers): wait for the leader vector, then apply it at the
	// same barrier. A missed push falls back to R/N — conservative.
	select {
	case <-ctx.Done():
		return
	case <-time.After(admissionAllocWait):
	}
	agg.FollowerReallocate()
}

func runAdmissionReloadPoller(ctx context.Context, store admissionStore) {
	ticker := time.NewTicker(admissionReloadPollInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := globalAdmissionSys.reloadIfChanged(ctx, store); err != nil {
				internalLogIf(ctx, err)
			}
		}
	}
}
