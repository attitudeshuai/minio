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
	"encoding/json"
	"errors"
	"fmt"
	"io"

	"github.com/minio/minio/internal/admission"
)

// Persisted request-admission rules live as one JSON file in the cluster
// metadata bucket. The file is small (one entry per rule) and is rewritten
// in full on every set/delete.
const (
	admissionRulesConfigFile  = "admission/rules.json"
	admissionRulesFileVersion = 1
	admissionRulesMaxBodySize = 1 << 20
)

// admissionStore is the subset of ObjectLayer needed by this layer.
type admissionStore interface {
	objectIO
	objectDeleter
	GetBucketInfo(ctx context.Context, bucket string, opts BucketOptions) (BucketInfo, error)
}

// admissionPeersReloadHook is wired by the cluster layer (admission-peer.go):
// after a local mutation it broadcasts a reload to all peer nodes. A nil hook
// (single-node / grid not up) is a no-op.
var admissionPeersReloadHook func()

type admissionRulesFile struct {
	Version int             `json:"version"`
	Rules   admission.Rules `json:"rules"`
}

// loadAdmissionRulesFromStore reads the rules file. A missing file is not an
// error: it returns nil rules. A corrupt/unreadable file returns an error so
// the caller can fail open and log it.
func loadAdmissionRulesFromStore(ctx context.Context, store objectIO) (admission.Rules, error) {
	data, err := readConfig(ctx, store, admissionRulesConfigFile)
	if err != nil {
		if errors.Is(err, errConfigNotFound) {
			return nil, nil
		}
		return nil, err
	}
	var f admissionRulesFile
	if err := json.Unmarshal(data, &f); err != nil {
		return nil, fmt.Errorf("parsing %s: %w", admissionRulesConfigFile, err)
	}
	if err := f.Rules.Validate(); err != nil {
		return nil, fmt.Errorf("invalid rules in %s: %w", admissionRulesConfigFile, err)
	}
	return f.Rules, nil
}

// saveAdmissionRulesToStore writes the rules file. An empty rule set removes
// the file instead.
func saveAdmissionRulesToStore(ctx context.Context, store objectIO, deleter objectDeleter, rules admission.Rules) error {
	if len(rules) == 0 {
		if err := deleteConfig(ctx, deleter, admissionRulesConfigFile); err != nil && !errors.Is(err, errConfigNotFound) {
			return err
		}
		return nil
	}
	f := admissionRulesFile{Version: admissionRulesFileVersion, Rules: rules}
	data, err := json.MarshalIndent(&f, "", "  ")
	if err != nil {
		return err
	}
	return saveConfig(ctx, store, admissionRulesConfigFile, data)
}

// initFromStore loads persisted rules at startup and applies them. Load
// failures are logged and the layer stays disabled (fail open).
func (sys *admissionSys) initFromStore(ctx context.Context, objAPI admissionStore) {
	rules, err := loadAdmissionRulesFromStore(ctx, objAPI)
	if err != nil {
		internalLogIf(ctx, fmt.Errorf("unable to load request admission rules, layer starts disabled: %w", err))
		return
	}
	if err := sys.replaceRules(rules); err != nil {
		internalLogIf(ctx, fmt.Errorf("unable to apply persisted request admission rules: %w", err))
	}
}

// reloadFromStore is the peer-reload entry: reload + reapply.
func (sys *admissionSys) reloadFromStore(ctx context.Context, store objectIO) error {
	rules, err := loadAdmissionRulesFromStore(ctx, store)
	if err != nil {
		return err
	}
	return sys.replaceRules(rules)
}

// errAdmissionNotLeader is returned when a mutating request reached a node
// that is not the current admission leader.
var errAdmissionNotLeader = errors.New("request admission leader changed; retry")

// isLeaderOrStandalone reports whether this node may write the rules file:
// the elected allocation leader, or a node with no cluster aggregator yet
// (single node / workers not started).
func (sys *admissionSys) isLeaderOrStandalone() bool {
	a := sys.agg.Load()
	if a == nil {
		return true
	}
	return a.IsLeader()
}

// leaderHost returns the current leader's host:port id (or local when
// standalone).
func (sys *admissionSys) leaderHost() string {
	if a := sys.agg.Load(); a != nil {
		return a.LeaderID()
	}
	return globalLocalNodeName
}

// forwardMutationToLeader sends a rule mutation to the leader node.
func (sys *admissionSys) forwardMutationToLeader(ctx context.Context, m admission.Mutation) error {
	host := sys.leaderHost()
	if host == globalLocalNodeName || globalNotificationSys == nil {
		return errAdmissionNotLeader
	}
	for _, client := range globalNotificationSys.peerClients {
		if client == nil || client.host == nil || client.host.String() != host {
			continue
		}
		return client.MutateAdmissionRules(ctx, m)
	}
	return errAdmissionNotLeader
}

// applyLeaderMutation is the leader-side handler for a forwarded mutation.
func (sys *admissionSys) applyLeaderMutation(ctx context.Context, m admission.Mutation) error {
	if !sys.isLeaderOrStandalone() {
		return errAdmissionNotLeader
	}
	objAPI := sys.backingStore()
	if objAPI == nil {
		return errServerNotInitialized
	}
	switch m.Op {
	case admission.MutationUpsert:
		return sys.leaderUpsert(ctx, objAPI, m.Rule)
	case admission.MutationDelete:
		return sys.leaderDelete(ctx, objAPI, m.Rule.Scope, m.Rule.Name)
	default:
		return fmt.Errorf("unknown admission mutation op %q", m.Op)
	}
}

// upsertRule routes an upsert to the leader (single writer) or executes it
// locally on the leader/standalone. Followers never write the rules file.
func (sys *admissionSys) upsertRule(ctx context.Context, objAPI admissionStore, rule admission.Rule) error {
	if err := rule.Validate(); err != nil {
		return err
	}
	if err := validateAdmissionTarget(rule.Scope, rule.Name, objAPI, ctx); err != nil {
		return err
	}
	warnIfBurstBelowNodes(rule)
	if !sys.isLeaderOrStandalone() {
		return sys.forwardMutationToLeader(ctx, admission.Mutation{Op: admission.MutationUpsert, Rule: rule})
	}
	return sys.leaderUpsert(ctx, objAPI, rule)
}

// warnIfBurstBelowNodes warns once per set when a burst allowance is smaller
// than the node count: per-node fractional burst shares can then never
// accumulate a whole token, making the rule effectively stricter than burst.
func warnIfBurstBelowNodes(rule admission.Rule) {
	n := totalNodeCount()
	if n <= 1 {
		return
	}
	for _, q := range []*admission.ClassQuota{rule.Total, rule.Reads, rule.Writes, rule.Lists} {
		if q != nil && q.Burst < n {
			internalLogIf(context.Background(), fmt.Errorf(
				"request admission rule %s/%s has burst %d smaller than node count %d; "+
					"set burst >= %d to preserve cluster-wide burst behavior",
				rule.Scope, rule.Name, q.Burst, n, n))
			return
		}
	}
}

// deleteRule routes a delete to the leader or executes it locally.
func (sys *admissionSys) deleteRule(ctx context.Context, objAPI admissionStore, scope admission.Scope, name string) error {
	if !sys.isLeaderOrStandalone() {
		return sys.forwardMutationToLeader(ctx, admission.Mutation{
			Op:   admission.MutationDelete,
			Rule: admission.Rule{Scope: scope, Name: name},
		})
	}
	return sys.leaderDelete(ctx, objAPI, scope, name)
}

// leaderUpsert validates and inserts/replaces one rule on the leader,
// persists the full set, applies it and broadcasts a peer reload. The
// previous rule set remains active on any failure.
//
// The merge base is re-read from the durable rules file under the mutation
// lock: two nodes briefly both seeing themselves as leader (cold start or a
// leader flap) then converge from shared state instead of clobbering each
// other's in-memory writes.
func (sys *admissionSys) leaderUpsert(ctx context.Context, objAPI admissionStore, rule admission.Rule) error {
	sys.upsert.Lock()
	defer sys.upsert.Unlock()

	next, err := loadAdmissionRulesFromStore(ctx, objAPI)
	if err != nil {
		return err
	}
	replaced := false
	for i := range next {
		if next[i].Scope == rule.Scope && next[i].Name == rule.Name {
			next[i] = rule
			replaced = true
			break
		}
	}
	if !replaced {
		next = append(next, rule)
	}
	if err := next.Validate(); err != nil {
		return err
	}

	// Persist first; local state changes only after a durable write.
	if err := saveAdmissionRulesToStore(ctx, objAPI, objAPI, next); err != nil {
		return err
	}
	if err := sys.replaceRules(next); err != nil {
		return err
	}
	if admissionPeersReloadHook != nil {
		admissionPeersReloadHook()
	}
	return nil
}

// leaderDelete removes one rule on the leader, persists, applies locally and
// broadcasts a reload. The merge base is re-read from the store under the
// mutation lock, mirroring leaderUpsert.
func (sys *admissionSys) leaderDelete(ctx context.Context, objAPI admissionStore, scope admission.Scope, name string) error {
	sys.upsert.Lock()
	defer sys.upsert.Unlock()

	cur, err := loadAdmissionRulesFromStore(ctx, objAPI)
	if err != nil {
		return err
	}
	out := make(admission.Rules, 0, len(cur))
	found := false
	for _, r := range cur {
		if r.Scope == scope && r.Name == name {
			found = true
			continue
		}
		out = append(out, r)
	}
	if !found {
		return errAdmissionRuleNotFound
	}

	if err := saveAdmissionRulesToStore(ctx, objAPI, objAPI, out); err != nil {
		return err
	}
	if err := sys.replaceRules(out); err != nil {
		return err
	}
	if admissionPeersReloadHook != nil {
		admissionPeersReloadHook()
	}
	return nil
}

// validateAdmissionTarget validates the target name of a rule. Bucket rules
// require an existing bucket (fail the set early); caller names are only
// syntax-checked (deleted principals are pruned later by the reaper).
func validateAdmissionTarget(scope admission.Scope, name string, objAPI admissionStore, ctx context.Context) error {
	switch scope {
	case admission.ScopeCaller:
		if name == "" {
			return errors.New("caller name must not be empty")
		}
		return nil
	case admission.ScopeBucket:
		if !IsValidBucketName(name) {
			return fmt.Errorf("invalid bucket name %q", name)
		}
		if _, err := objAPI.GetBucketInfo(ctx, name, BucketOptions{}); err != nil {
			return err
		}
		return nil
	default:
		return fmt.Errorf("invalid scope %q", scope)
	}
}

// errAdmissionRuleNotFound is returned when deleting an unknown rule.
var errAdmissionRuleNotFound = errors.New("request admission rule not found")

// readAdmissionBody reads and size-limits an admin request body.
func readAdmissionBody(r io.Reader) ([]byte, error) {
	return io.ReadAll(io.LimitReader(r, admissionRulesMaxBodySize+1))
}

// reloadIfChanged reloads rules from the store and applies them only when
// they differ from the current set. It backs the 30s fallback poller.
func (sys *admissionSys) reloadIfChanged(ctx context.Context, store admissionStore) error {
	rules, err := loadAdmissionRulesFromStore(ctx, store)
	if err != nil {
		return err
	}
	if rulesEqual(rules, sys.currentRules()) {
		return nil
	}
	return sys.replaceRules(rules)
}

// rulesEqual compares two rule sets independent of ordering.
func rulesEqual(a, b admission.Rules) bool {
	if len(a) != len(b) {
		return false
	}
	for _, ra := range a {
		rb, ok := b.Find(ra.Scope, ra.Name)
		if !ok || !ruleEquals(ra, rb) {
			return false
		}
	}
	return true
}

func quotaEqual(x, y *admission.ClassQuota) bool {
	if x == nil || y == nil {
		return x == y
	}
	if x.Rate != y.Rate || x.Burst != y.Burst {
		return false
	}
	if (x.Wait == nil) != (y.Wait == nil) {
		return false
	}
	return x.Wait == nil || *x.Wait == *y.Wait
}

func ruleEquals(x, y admission.Rule) bool {
	return x.Scope == y.Scope && x.Name == y.Name &&
		quotaEqual(x.Total, y.Total) &&
		quotaEqual(x.Reads, y.Reads) &&
		quotaEqual(x.Writes, y.Writes) &&
		quotaEqual(x.Lists, y.Lists)
}
