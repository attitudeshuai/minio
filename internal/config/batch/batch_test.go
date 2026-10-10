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

package batch

import (
	"testing"
	"time"

	"github.com/minio/minio/internal/config"
)

// kvsWith returns DefaultKVS with the given overrides applied.
func kvsWith(overrides map[string]string) config.KVS {
	out := make(config.KVS, 0, len(DefaultKVS))
	for _, kv := range DefaultKVS {
		if v, ok := overrides[kv.Key]; ok {
			out = append(out, config.KV{Key: kv.Key, Value: v})
			delete(overrides, kv.Key)
			continue
		}
		out = append(out, kv)
	}
	for k, v := range overrides {
		out = append(out, config.KV{Key: k, Value: v})
	}
	return out
}

func TestLookupConfigDefaults(t *testing.T) {
	cfg, err := LookupConfig(DefaultKVS)
	if err != nil {
		t.Fatalf("LookupConfig(defaults) failed: %v", err)
	}
	if cfg.MaxJobs != 100 {
		t.Errorf("MaxJobs default = %d, want 100", cfg.MaxJobs)
	}
	if cfg.QueueCapacity != 10000 {
		t.Errorf("QueueCapacity default = %d, want 10000", cfg.QueueCapacity)
	}
	if cfg.ReplicationMaxJobs != 0 || cfg.KeyRotationMaxJobs != 0 || cfg.ExpirationMaxJobs != 0 {
		t.Errorf("per-type max jobs default must be 0 (unlimited), got repl=%d kr=%d exp=%d",
			cfg.ReplicationMaxJobs, cfg.KeyRotationMaxJobs, cfg.ExpirationMaxJobs)
	}
	if cfg.CPUWatermark != 0 || cfg.MemWatermark != 0 || cfg.DriveWatermark != 0 {
		t.Errorf("watermarks default must be 0 (disabled), got cpu=%d mem=%d drive=%d",
			cfg.CPUWatermark, cfg.MemWatermark, cfg.DriveWatermark)
	}
	if cfg.ResourceThrottleWait != time.Second {
		t.Errorf("ResourceThrottleWait default = %v, want 1s", cfg.ResourceThrottleWait)
	}
	if cfg.ReplicationPriority != PriorityNormal || cfg.KeyRotationPriority != PriorityNormal || cfg.ExpirationPriority != PriorityNormal {
		t.Errorf("default priorities must be normal, got repl=%q kr=%q exp=%q",
			cfg.ReplicationPriority, cfg.KeyRotationPriority, cfg.ExpirationPriority)
	}
	if cfg.ReplicationWorkersWait != 0 || cfg.KeyRotationWorkersWait != 0 || cfg.ExpirationWorkersWait != 0 {
		t.Errorf("workers wait defaults must be zero")
	}
}

func TestLookupConfigInvalid(t *testing.T) {
	cases := []struct {
		key, value string
	}{
		{MaxJobs, "-1"},
		{MaxJobs, "0"},
		{MaxJobs, "abc"},
		{QueueCapacity, "0"},
		{ReplicationMaxJobs, "-2"},
		{KeyRotationMaxJobs, "x"},
		{ExpirationMaxJobs, "-1"},
		{CPUWatermark, "101"},
		{MemWatermark, "-1"},
		{DriveWatermark, "hot"},
		{ResourceThrottleWait, "-1s"},
		{ReplicationPriority, "urgent"},
		{KeyRotationPriority, "HIGHEST"},
	}
	for i, tc := range cases {
		kvs := kvsWith(map[string]string{tc.key: tc.value})
		if _, err := LookupConfig(kvs); err == nil {
			t.Errorf("case %d (%s=%s): expected error, got nil", i, tc.key, tc.value)
		}
	}
}

func TestLookupConfigBoundaries(t *testing.T) {
	cfg, err := LookupConfig(kvsWith(map[string]string{
		MaxJobs:              "1",
		QueueCapacity:        "1",
		ReplicationMaxJobs:   "100",
		CPUWatermark:         "1",
		DriveWatermark:       "100",
		ResourceThrottleWait: "0s",
		ReplicationPriority:  "HIGH",
	}))
	if err != nil {
		t.Fatalf("boundary-valid config failed: %v", err)
	}
	if cfg.MaxJobs != 1 || cfg.QueueCapacity != 1 || cfg.ReplicationMaxJobs != 100 {
		t.Errorf("int boundary values mismatch: %+v", cfg)
	}
	if cfg.CPUWatermark != 1 || cfg.DriveWatermark != 100 {
		t.Errorf("watermark boundary values mismatch: cpu=%d drive=%d", cfg.CPUWatermark, cfg.DriveWatermark)
	}
	if cfg.ResourceThrottleWait != 0 {
		t.Errorf("throttle wait = %v, want 0", cfg.ResourceThrottleWait)
	}
	if cfg.ReplicationPriority != PriorityHigh {
		t.Errorf("priority = %q, want high", cfg.ReplicationPriority)
	}
}

func TestCloneAndUpdate(t *testing.T) {
	cfg, err := LookupConfig(DefaultKVS)
	if err != nil {
		t.Fatal(err)
	}
	clone := cfg.Clone()
	if clone != cfg {
		t.Errorf("Clone() mismatch: %+v != %+v", clone, cfg)
	}
	clone.MaxJobs = 3
	if cfg.MaxJobs != 100 {
		t.Errorf("mutating clone must not affect original config")
	}

	cfg.MaxJobs = 7
	cfg.Update(clone)
	if cfg.MaxJobs != 3 {
		t.Errorf("Update did not apply MaxJobs, got %d", cfg.MaxJobs)
	}
}

func TestNormalizePriority(t *testing.T) {
	want := map[string]string{
		"":       PriorityNormal,
		"normal": PriorityNormal,
		"NORMAL": PriorityNormal,
		" High ": PriorityHigh,
		"low":    PriorityLow,
	}
	for in, out := range want {
		got, err := NormalizePriority(in)
		if err != nil || got != out {
			t.Errorf("NormalizePriority(%q) = %q, %v; want %q", in, got, err, out)
		}
	}
	if _, err := NormalizePriority("urgent"); err == nil {
		t.Errorf("urgent must be rejected")
	}
}
