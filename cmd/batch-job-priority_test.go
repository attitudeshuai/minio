// Copyright (c) 2015-2024 MinIO, Inc.
//
// This file is part of MinIO Object Storage stack
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Affero General Public License for more details.
//
// You should have received a copy of the GNU Affero General Public License
// along with this program.  If not, see <http://www.gnu.org/licenses/>.

package cmd

import (
	"context"
	"strings"
	"testing"

	"github.com/tinylib/msgp/msgp"
	"gopkg.in/yaml.v3"
)

func TestNormalizeBatchPriority(t *testing.T) {
	cases := []struct {
		in   string
		want batchPriority
		err  bool
	}{
		{"", batchPriorityUnspecified, false},
		{"normal", batchPriorityNormal, false},
		{"NORMAL", batchPriorityNormal, false},
		{" High ", batchPriorityHigh, false},
		{"low", batchPriorityLow, false},
		{"urgent", batchPriorityUnspecified, true},
		{"1", batchPriorityUnspecified, true},
	}
	for _, tc := range cases {
		got, err := normalizeBatchPriority(tc.in)
		if tc.err {
			if err == nil {
				t.Errorf("normalizeBatchPriority(%q) expected error", tc.in)
			}
			continue
		}
		if err != nil || got != tc.want {
			t.Errorf("normalizeBatchPriority(%q) = %q, %v; want %q", tc.in, got, err, tc.want)
		}
	}
	if w := (batchPriorityHigh).weight(); w != batchPriorityWeightHigh {
		t.Errorf("high weight = %d", w)
	}
	if w := (batchPriorityLow).weight(); w != batchPriorityWeightLow {
		t.Errorf("low weight = %d", w)
	}
	if w := (batchPriorityUnspecified).weight(); w != batchPriorityWeightNormal {
		t.Errorf("unspecified weight = %d, want normal", w)
	}
}

func TestBatchJobRequestPriorityYAML(t *testing.T) {
	var w struct {
		Priority string `yaml:"priority"`
	}
	if err := yaml.Unmarshal([]byte("priority: HIGH\n"), &w); err != nil {
		t.Fatal(err)
	}
	p, err := normalizeBatchPriority(w.Priority)
	if err != nil || p != batchPriorityHigh {
		t.Fatalf("HIGH should normalize to high, got %q err=%v", p, err)
	}
}

func TestBatchJobRequestValidatePriority(t *testing.T) {
	// Invalid priority is rejected before inspecting the job payload.
	j := BatchJobRequest{Priority: "urgent"}
	err := j.Validate(context.Background(), nil)
	if err == nil || !strings.Contains(err.Error(), "priority") {
		t.Fatalf("expected priority validation error, got %v", err)
	}

	// Empty priority passes priority validation (nil layer then fails on
	// missing payload with errInvalidArgument, not a priority error).
	j = BatchJobRequest{}
	err = j.Validate(context.Background(), nil)
	if err != nil && strings.Contains(err.Error(), "priority") {
		t.Fatalf("empty priority should not fail validation, got %v", err)
	}
}

func TestBatchJobRequestPriorityMsgpRoundtrip(t *testing.T) {
	j := BatchJobRequest{ID: "replicate-x", User: "alice", Priority: "high"}
	bts, err := j.MarshalMsg(nil)
	if err != nil {
		t.Fatal(err)
	}
	var got BatchJobRequest
	if _, err := got.UnmarshalMsg(bts); err != nil {
		t.Fatal(err)
	}
	if got.Priority != "high" || got.ID != "replicate-x" || got.User != "alice" {
		t.Fatalf("roundtrip mismatch: %+v", got)
	}
}

// TestBatchJobRequestMsgpUnknownFieldTolerance verifies that future/unknown
// msgp fields (as produced by newer versions) are skipped while known fields
// still decode - the compatibility property the appended "pri" field relies
// on in both directions.
func TestBatchJobRequestMsgpUnknownFieldTolerance(t *testing.T) {
	var b []byte
	b = msgp.AppendMapHeader(b, 2)
	b = msgp.AppendString(b, "future_field")
	b = msgp.AppendString(b, "ignored")
	b = msgp.AppendString(b, "pri")
	b = msgp.AppendString(b, "low")

	var got BatchJobRequest
	if _, err := got.UnmarshalMsg(b); err != nil {
		t.Fatalf("unknown field should be skipped: %v", err)
	}
	if got.Priority != "low" {
		t.Fatalf("Priority = %q, want low", got.Priority)
	}

	// Payload without the pri field (produced by an older server) decodes
	// to empty priority, which is treated as normal/default at admission.
	b = b[:0]
	b = msgp.AppendMapHeader(b, 1)
	b = msgp.AppendString(b, "ID")
	b = msgp.AppendString(b, "expire-old")
	got = BatchJobRequest{}
	if _, err := got.UnmarshalMsg(b); err != nil {
		t.Fatalf("old payload should decode: %v", err)
	}
	if got.Priority != "" || got.ID != "expire-old" {
		t.Fatalf("old payload mismatch: %+v", got)
	}
}
