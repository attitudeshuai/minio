// Copyright (c) 2015-2023 MinIO, Inc.
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
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/minio/minio/internal/config"
	"github.com/minio/pkg/v3/env"
)

// Batch job environment variables
const (
	ReplicationWorkersWait = "replication_workers_wait"
	KeyRotationWorkersWait = "keyrotation_workers_wait"
	ExpirationWorkersWait  = "expiration_workers_wait"

	// MaxJobs is the maximum number of batch jobs that may run
	// concurrently on this server's batch job pool.
	MaxJobs = "max_jobs"
	// ReplicationMaxJobs bounds concurrently running replication jobs.
	ReplicationMaxJobs = "replication_max_jobs"
	// KeyRotationMaxJobs bounds concurrently running keyrotation jobs.
	KeyRotationMaxJobs = "keyrotation_max_jobs"
	// ExpirationMaxJobs bounds concurrently running expiration jobs.
	ExpirationMaxJobs = "expiration_max_jobs"
	// QueueCapacity is the number of jobs that may wait for admission
	// before further submissions are rejected.
	QueueCapacity = "queue_capacity"

	// CPUWatermark is the system CPU usage percentage (load1/numCPU)
	// above which fresh batch jobs are queued instead of started.
	CPUWatermark = "cpu_watermark"
	// MemWatermark is the used memory percentage above which fresh
	// batch jobs are queued instead of started.
	MemWatermark = "mem_watermark"
	// DriveWatermark is the used drive space percentage above which
	// fresh batch jobs are queued instead of started.
	DriveWatermark = "drive_watermark"
	// ResourceThrottleWait is the extra per-object delay applied to
	// running batch jobs while any resource watermark is breached.
	ResourceThrottleWait = "resource_throttle_wait"

	// ReplicationPriority is the default priority assigned to
	// replication jobs that do not declare one explicitly.
	ReplicationPriority = "replication_priority"
	// KeyRotationPriority is the default priority assigned to
	// keyrotation jobs that do not declare one explicitly.
	KeyRotationPriority = "keyrotation_priority"
	// ExpirationPriority is the default priority assigned to expiration
	// jobs that do not declare one explicitly.
	ExpirationPriority = "expiration_priority"

	EnvReplicationWorkersWait   = "MINIO_BATCH_REPLICATION_WORKERS_WAIT"
	EnvKeyRotationWorkersWait   = "MINIO_BATCH_KEYROTATION_WORKERS_WAIT"
	EnvKeyExpirationWorkersWait = "MINIO_BATCH_EXPIRATION_WORKERS_WAIT"

	EnvMaxJobs              = "MINIO_BATCH_MAX_JOBS"
	EnvReplicationMaxJobs   = "MINIO_BATCH_REPLICATION_MAX_JOBS"
	EnvKeyRotationMaxJobs   = "MINIO_BATCH_KEYROTATION_MAX_JOBS"
	EnvExpirationMaxJobs    = "MINIO_BATCH_EXPIRATION_MAX_JOBS"
	EnvQueueCapacity        = "MINIO_BATCH_QUEUE_CAPACITY"
	EnvCPUWatermark         = "MINIO_BATCH_CPU_WATERMARK"
	EnvMemWatermark         = "MINIO_BATCH_MEM_WATERMARK"
	EnvDriveWatermark       = "MINIO_BATCH_DRIVE_WATERMARK"
	EnvResourceThrottleWait = "MINIO_BATCH_RESOURCE_THROTTLE_WAIT"
	EnvReplicationPriority  = "MINIO_BATCH_REPLICATION_PRIORITY"
	EnvKeyRotationPriority  = "MINIO_BATCH_KEYROTATION_PRIORITY"
	EnvExpirationPriority   = "MINIO_BATCH_EXPIRATION_PRIORITY"
)

// Priority values accepted by the *_priority configuration keys.
const (
	PriorityLow    = "low"
	PriorityNormal = "normal"
	PriorityHigh   = "high"
)

// Default values preserve the pre-admission-controller behavior:
// 100 job workers, a 10000 deep waiting buffer and resource gating off.
const (
	defaultMaxJobs         = 100
	defaultQueueCapacity   = 10000
	defaultThrottleWait    = time.Second
	maxWatermarkPercentage = 100
)

var configMu sync.RWMutex

// Config represents the batch job settings.
type Config struct {
	ReplicationWorkersWait time.Duration `json:"replicationWorkersWait"`
	KeyRotationWorkersWait time.Duration `json:"keyRotationWorkersWait"`
	ExpirationWorkersWait  time.Duration `json:"expirationWorkersWait"`

	MaxJobs            int `json:"maxJobs"`
	ReplicationMaxJobs int `json:"replicationMaxJobs"`
	KeyRotationMaxJobs int `json:"keyRotationMaxJobs"`
	ExpirationMaxJobs  int `json:"expirationMaxJobs"`
	QueueCapacity      int `json:"queueCapacity"`

	CPUWatermark   int `json:"cpuWatermark"`
	MemWatermark   int `json:"memWatermark"`
	DriveWatermark int `json:"driveWatermark"`

	ResourceThrottleWait time.Duration `json:"resourceThrottleWait"`

	ReplicationPriority string `json:"replicationPriority"`
	KeyRotationPriority string `json:"keyRotationPriority"`
	ExpirationPriority  string `json:"expirationPriority"`
}

// NormalizePriority lower-cases and validates a priority value. An empty
// value is treated as the default (normal).
func NormalizePriority(p string) (string, error) {
	p = strings.ToLower(strings.TrimSpace(p))
	switch p {
	case "":
		return PriorityNormal, nil
	case PriorityLow, PriorityNormal, PriorityHigh:
		return p, nil
	}
	return "", fmt.Errorf("invalid batch job priority %q: expected one of low, normal, high", p)
}

// ExpirationWait returns the duration for which a batch expiration worker
// would wait before working on next object.
func (opts Config) ExpirationWait() time.Duration {
	configMu.RLock()
	defer configMu.RUnlock()

	return opts.ExpirationWorkersWait
}

// ReplicationWait returns the duration for which a batch replication worker
// would wait before working on next object.
func (opts Config) ReplicationWait() time.Duration {
	configMu.RLock()
	defer configMu.RUnlock()

	return opts.ReplicationWorkersWait
}

// KeyRotationWait returns the duration for which a batch key-rotation worker
// would wait before working on next object.
func (opts Config) KeyRotationWait() time.Duration {
	configMu.RLock()
	defer configMu.RUnlock()

	return opts.KeyRotationWorkersWait
}

// Clone returns a copy of Config value
func (opts Config) Clone() Config {
	configMu.RLock()
	defer configMu.RUnlock()

	return Config{
		ReplicationWorkersWait: opts.ReplicationWorkersWait,
		KeyRotationWorkersWait: opts.KeyRotationWorkersWait,
		ExpirationWorkersWait:  opts.ExpirationWorkersWait,

		MaxJobs:            opts.MaxJobs,
		ReplicationMaxJobs: opts.ReplicationMaxJobs,
		KeyRotationMaxJobs: opts.KeyRotationMaxJobs,
		ExpirationMaxJobs:  opts.ExpirationMaxJobs,
		QueueCapacity:      opts.QueueCapacity,

		CPUWatermark:   opts.CPUWatermark,
		MemWatermark:   opts.MemWatermark,
		DriveWatermark: opts.DriveWatermark,

		ResourceThrottleWait: opts.ResourceThrottleWait,

		ReplicationPriority: opts.ReplicationPriority,
		KeyRotationPriority: opts.KeyRotationPriority,
		ExpirationPriority:  opts.ExpirationPriority,
	}
}

// Limits carries the admission concurrency limits.
type Limits struct {
	MaxJobs            int
	ReplicationMaxJobs int
	KeyRotationMaxJobs int
	ExpirationMaxJobs  int
	QueueCapacity      int
}

// Limits returns the currently configured admission limits.
func (opts Config) Limits() Limits {
	configMu.RLock()
	defer configMu.RUnlock()

	return Limits{
		MaxJobs:            opts.MaxJobs,
		ReplicationMaxJobs: opts.ReplicationMaxJobs,
		KeyRotationMaxJobs: opts.KeyRotationMaxJobs,
		ExpirationMaxJobs:  opts.ExpirationMaxJobs,
		QueueCapacity:      opts.QueueCapacity,
	}
}

// Watermarks carries the resource gating settings.
type Watermarks struct {
	CPU                 int
	Mem                 int
	Drive               int
	ResourceThrottleWait time.Duration
}

// Watermarks returns the currently configured resource watermarks.
func (opts Config) Watermarks() Watermarks {
	configMu.RLock()
	defer configMu.RUnlock()

	return Watermarks{
		CPU:                  opts.CPUWatermark,
		Mem:                  opts.MemWatermark,
		Drive:                opts.DriveWatermark,
		ResourceThrottleWait: opts.ResourceThrottleWait,
	}
}

// TypePriority returns the default scheduling priority for a job type key
// ("replicate", "keyrotate" or "expire"); unknown keys resolve to normal.
func (opts Config) TypePriority(typ string) string {
	configMu.RLock()
	defer configMu.RUnlock()

	switch typ {
	case "replicate":
		return opts.ReplicationPriority
	case "keyrotate":
		return opts.KeyRotationPriority
	case "expire":
		return opts.ExpirationPriority
	}
	return PriorityNormal
}

// Update updates opts with nopts
func (opts *Config) Update(nopts Config) {
	configMu.Lock()
	defer configMu.Unlock()

	opts.ReplicationWorkersWait = nopts.ReplicationWorkersWait
	opts.KeyRotationWorkersWait = nopts.KeyRotationWorkersWait
	opts.ExpirationWorkersWait = nopts.ExpirationWorkersWait

	opts.MaxJobs = nopts.MaxJobs
	opts.ReplicationMaxJobs = nopts.ReplicationMaxJobs
	opts.KeyRotationMaxJobs = nopts.KeyRotationMaxJobs
	opts.ExpirationMaxJobs = nopts.ExpirationMaxJobs
	opts.QueueCapacity = nopts.QueueCapacity

	opts.CPUWatermark = nopts.CPUWatermark
	opts.MemWatermark = nopts.MemWatermark
	opts.DriveWatermark = nopts.DriveWatermark

	opts.ResourceThrottleWait = nopts.ResourceThrottleWait

	opts.ReplicationPriority = nopts.ReplicationPriority
	opts.KeyRotationPriority = nopts.KeyRotationPriority
	opts.ExpirationPriority = nopts.ExpirationPriority
}

// DefaultKVS - default KV config for batch job settings
var DefaultKVS = config.KVS{
	config.KV{
		Key:   ReplicationWorkersWait,
		Value: "0ms", // No wait by default between each replication attempts.
	},
	config.KV{
		Key:   KeyRotationWorkersWait,
		Value: "0ms", // No wait by default between each key rotation attempts.
	},
	config.KV{
		Key:   ExpirationWorkersWait,
		Value: "0ms", // No wait by default between each expiration attempts.
	},
	config.KV{
		Key:   MaxJobs,
		Value: strconv.Itoa(defaultMaxJobs),
	},
	config.KV{
		Key:   ReplicationMaxJobs,
		Value: "0", // 0 means unlimited.
	},
	config.KV{
		Key:   KeyRotationMaxJobs,
		Value: "0",
	},
	config.KV{
		Key:   ExpirationMaxJobs,
		Value: "0",
	},
	config.KV{
		Key:   QueueCapacity,
		Value: strconv.Itoa(defaultQueueCapacity),
	},
	config.KV{
		Key:   CPUWatermark,
		Value: "0", // 0 disables CPU based gating.
	},
	config.KV{
		Key:   MemWatermark,
		Value: "0",
	},
	config.KV{
		Key:   DriveWatermark,
		Value: "0",
	},
	config.KV{
		Key:   ResourceThrottleWait,
		Value: defaultThrottleWait.String(),
	},
	config.KV{
		Key:   ReplicationPriority,
		Value: PriorityNormal,
	},
	config.KV{
		Key:   KeyRotationPriority,
		Value: PriorityNormal,
	},
	config.KV{
		Key:   ExpirationPriority,
		Value: PriorityNormal,
	},
}

func lookupPositiveInt(kvs config.KVS, key, envName string) (int, error) {
	v, err := strconv.Atoi(env.Get(envName, kvs.GetWithDefault(key, DefaultKVS)))
	if err != nil {
		return 0, err
	}
	if v <= 0 {
		return 0, fmt.Errorf("%s must be a positive integer, got %d", key, v)
	}
	return v, nil
}

func lookupNonNegativeInt(kvs config.KVS, key, envName string) (int, error) {
	v, err := strconv.Atoi(env.Get(envName, kvs.GetWithDefault(key, DefaultKVS)))
	if err != nil {
		return 0, err
	}
	if v < 0 {
		return 0, fmt.Errorf("%s must be a non-negative integer, got %d", key, v)
	}
	return v, nil
}

func lookupWatermark(kvs config.KVS, key, envName string) (int, error) {
	v, err := strconv.Atoi(env.Get(envName, kvs.GetWithDefault(key, DefaultKVS)))
	if err != nil {
		return 0, err
	}
	if v < 0 || v > maxWatermarkPercentage {
		return 0, fmt.Errorf("%s must be between 0 (disabled) and 100, got %d", key, v)
	}
	return v, nil
}

// LookupConfig - lookup config and override with valid environment settings if any.
func LookupConfig(kvs config.KVS) (cfg Config, err error) {
	if err = config.CheckValidKeys(config.BatchSubSys, kvs, DefaultKVS); err != nil {
		return cfg, err
	}

	cfg.ReplicationWorkersWait = 0
	cfg.KeyRotationWorkersWait = 0
	cfg.ExpirationWorkersWait = 0

	rduration, err := time.ParseDuration(env.Get(EnvReplicationWorkersWait, kvs.GetWithDefault(ReplicationWorkersWait, DefaultKVS)))
	if err != nil {
		return cfg, err
	}
	if rduration < 0 {
		return cfg, config.ErrInvalidBatchReplicationWorkersWait(nil)
	}

	kduration, err := time.ParseDuration(env.Get(EnvKeyRotationWorkersWait, kvs.GetWithDefault(KeyRotationWorkersWait, DefaultKVS)))
	if err != nil {
		return cfg, err
	}
	if kduration < 0 {
		return cfg, config.ErrInvalidBatchKeyRotationWorkersWait(nil)
	}

	eduration, err := time.ParseDuration(env.Get(EnvKeyExpirationWorkersWait, kvs.GetWithDefault(ExpirationWorkersWait, DefaultKVS)))
	if err != nil {
		return cfg, err
	}
	if eduration < 0 {
		return cfg, config.ErrInvalidBatchExpirationWorkersWait(nil)
	}

	if rduration > 0 {
		cfg.ReplicationWorkersWait = rduration
	}
	if kduration > 0 {
		cfg.KeyRotationWorkersWait = kduration
	}
	if eduration > 0 {
		cfg.ExpirationWorkersWait = eduration
	}

	if cfg.MaxJobs, err = lookupPositiveInt(kvs, MaxJobs, EnvMaxJobs); err != nil {
		return cfg, config.ErrInvalidBatchMaxJobs(nil)
	}
	if cfg.QueueCapacity, err = lookupPositiveInt(kvs, QueueCapacity, EnvQueueCapacity); err != nil {
		return cfg, config.ErrInvalidBatchQueueCapacity(nil)
	}
	if cfg.ReplicationMaxJobs, err = lookupNonNegativeInt(kvs, ReplicationMaxJobs, EnvReplicationMaxJobs); err != nil {
		return cfg, config.ErrInvalidBatchMaxJobs(nil)
	}
	if cfg.KeyRotationMaxJobs, err = lookupNonNegativeInt(kvs, KeyRotationMaxJobs, EnvKeyRotationMaxJobs); err != nil {
		return cfg, config.ErrInvalidBatchMaxJobs(nil)
	}
	if cfg.ExpirationMaxJobs, err = lookupNonNegativeInt(kvs, ExpirationMaxJobs, EnvExpirationMaxJobs); err != nil {
		return cfg, config.ErrInvalidBatchMaxJobs(nil)
	}

	if cfg.CPUWatermark, err = lookupWatermark(kvs, CPUWatermark, EnvCPUWatermark); err != nil {
		return cfg, config.ErrInvalidBatchWatermark(nil)
	}
	if cfg.MemWatermark, err = lookupWatermark(kvs, MemWatermark, EnvMemWatermark); err != nil {
		return cfg, config.ErrInvalidBatchWatermark(nil)
	}
	if cfg.DriveWatermark, err = lookupWatermark(kvs, DriveWatermark, EnvDriveWatermark); err != nil {
		return cfg, config.ErrInvalidBatchWatermark(nil)
	}

	throttleWait, err := time.ParseDuration(env.Get(EnvResourceThrottleWait, kvs.GetWithDefault(ResourceThrottleWait, DefaultKVS)))
	if err != nil {
		return cfg, err
	}
	if throttleWait < 0 {
		return cfg, config.ErrInvalidBatchResourceThrottleWait(nil)
	}
	cfg.ResourceThrottleWait = throttleWait

	if cfg.ReplicationPriority, err = NormalizePriority(env.Get(EnvReplicationPriority, kvs.GetWithDefault(ReplicationPriority, DefaultKVS))); err != nil {
		return cfg, config.ErrInvalidBatchPriority(nil)
	}
	if cfg.KeyRotationPriority, err = NormalizePriority(env.Get(EnvKeyRotationPriority, kvs.GetWithDefault(KeyRotationPriority, DefaultKVS))); err != nil {
		return cfg, config.ErrInvalidBatchPriority(nil)
	}
	if cfg.ExpirationPriority, err = NormalizePriority(env.Get(EnvExpirationPriority, kvs.GetWithDefault(ExpirationPriority, DefaultKVS))); err != nil {
		return cfg, config.ErrInvalidBatchPriority(nil)
	}

	return cfg, nil
}
