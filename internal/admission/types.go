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

package admission

import (
	"encoding/json"
	"errors"
	"fmt"
	"time"
)

// Scope identifies the dimension a request admission rule applies to.
type Scope string

const (
	// ScopeBucket limits requests keyed by bucket name.
	ScopeBucket Scope = "bucket"
	// ScopeCaller limits requests keyed by caller principal.
	ScopeCaller Scope = "caller"
)

// ActionClass groups S3 API actions into rate-limit classes.
type ActionClass string

const (
	// ClassRead covers object/bucket read and metadata read actions.
	ClassRead ActionClass = "read"
	// ClassWrite covers object/bucket mutation actions.
	ClassWrite ActionClass = "write"
	// ClassList covers bucket/account listing actions.
	ClassList ActionClass = "list"
)

// ClassQuota is a rate+burst allowance for one class (or for the rule total).
type ClassQuota struct {
	// Rate is the cluster-wide token refill rate in requests per second.
	Rate float64 `json:"rate"`
	// Burst is the cluster-wide maximum number of tokens that may be
	// accumulated (and therefore admitted in a single burst).
	Burst int `json:"burst"`
	// Wait is the maximum time a request may be queued waiting for a
	// token before it is rejected. A nil pointer means the manager
	// default wait is used; an explicit zero duration means reject
	// immediately when no token is available.
	Wait *time.Duration `json:"wait_ms,omitempty"`
}

// waitJSON is the wire representation of ClassQuota.
type waitJSON struct {
	Rate   float64 `json:"rate"`
	Burst  int     `json:"burst"`
	WaitMS *int64  `json:"wait_ms,omitempty"`
}

// MarshalJSON encodes Wait in milliseconds.
func (q ClassQuota) MarshalJSON() ([]byte, error) {
	j := waitJSON{Rate: q.Rate, Burst: q.Burst}
	if q.Wait != nil {
		ms := q.Wait.Milliseconds()
		j.WaitMS = &ms
	}
	return json.Marshal(j)
}

// UnmarshalJSON decodes Wait from milliseconds.
func (q *ClassQuota) UnmarshalJSON(data []byte) error {
	// Reset optional fields so decoding into a reused struct does not leak
	// values from a previous document.
	q.Wait = nil
	var j waitJSON
	if err := json.Unmarshal(data, &j); err != nil {
		return err
	}
	q.Rate = j.Rate
	q.Burst = j.Burst
	if j.WaitMS != nil {
		w := time.Duration(*j.WaitMS) * time.Millisecond
		q.Wait = &w
	}
	return nil
}

// Validate checks a single quota allowance.
func (q ClassQuota) Validate() error {
	if q.Rate < 0 {
		return errors.New("rate must be >= 0")
	}
	if q.Burst < 1 {
		return errors.New("burst must be >= 1")
	}
	if q.Wait != nil && *q.Wait < 0 {
		return errors.New("wait must be >= 0")
	}
	return nil
}

// WaitOr returns the configured max wait, or the provided default when the
// wait was not specified.
func (q ClassQuota) WaitOr(def time.Duration) time.Duration {
	if q.Wait != nil {
		return *q.Wait
	}
	return def
}

// Rule is one admission rule for a (scope, name) pair. At least one of
// Total/Reads/Writes/Lists must be set.
type Rule struct {
	Scope Scope       `json:"scope"`
	Name  string      `json:"name"`
	Total *ClassQuota `json:"total,omitempty"`

	Reads  *ClassQuota `json:"reads,omitempty"`
	Writes *ClassQuota `json:"writes,omitempty"`
	Lists  *ClassQuota `json:"lists,omitempty"`
}

// quotaFor returns the quota for an action class, if configured.
func (r Rule) quotaFor(class ActionClass) *ClassQuota {
	switch class {
	case ClassRead:
		return r.Reads
	case ClassWrite:
		return r.Writes
	case ClassList:
		return r.Lists
	}
	return nil
}

// Validate checks the rule for well-formedness.
func (r Rule) Validate() error {
	switch r.Scope {
	case ScopeBucket, ScopeCaller:
	default:
		return fmt.Errorf("invalid scope %q", r.Scope)
	}
	if r.Name == "" {
		return errors.New("rule name must not be empty")
	}
	has := false
	for _, q := range []*ClassQuota{r.Total, r.Reads, r.Writes, r.Lists} {
		if q == nil {
			continue
		}
		if err := q.Validate(); err != nil {
			return err
		}
		has = true
	}
	if !has {
		return errors.New("at least one of total/reads/writes/lists must be set")
	}
	return nil
}

// Rules is a set of admission rules.
type Rules []Rule

// Validate validates all rules and guarantees (scope,name) uniqueness.
func (rs Rules) Validate() error {
	seen := make(map[string]struct{}, len(rs))
	for i, r := range rs {
		if err := r.Validate(); err != nil {
			return fmt.Errorf("rule %d (%s/%s): %w", i, r.Scope, r.Name, err)
		}
		key := string(r.Scope) + "/" + r.Name
		if _, ok := seen[key]; ok {
			return fmt.Errorf("duplicate rule for %s", key)
		}
		seen[key] = struct{}{}
	}
	return nil
}

// Find returns the rule for a scope/name pair.
func (rs Rules) Find(scope Scope, name string) (Rule, bool) {
	for _, r := range rs {
		if r.Scope == scope && r.Name == name {
			return r, true
		}
	}
	return Rule{}, false
}

func cloneQuota(q *ClassQuota) *ClassQuota {
	if q == nil {
		return nil
	}
	cp := *q
	if q.Wait != nil {
		w := *q.Wait
		cp.Wait = &w
	}
	return &cp
}

// Clone returns a deep copy that is safe to mutate, including the quota
// pointers.
func (rs Rules) Clone() Rules {
	out := make(Rules, len(rs))
	for i, r := range rs {
		out[i] = Rule{
			Scope:  r.Scope,
			Name:   r.Name,
			Total:  cloneQuota(r.Total),
			Reads:  cloneQuota(r.Reads),
			Writes: cloneQuota(r.Writes),
			Lists:  cloneQuota(r.Lists),
		}
	}
	return out
}
