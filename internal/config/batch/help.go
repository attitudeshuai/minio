// Copyright (c) 2015-2023 MinIO, Inc.
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

package batch

import "github.com/minio/minio/internal/config"

// Help template for batch feature.
var (
	defaultHelpPostfix = func(key string) string {
		return config.DefaultHelpPostfix(DefaultKVS, key)
	}

	// Help provides help for config values
	Help = config.HelpKVS{
		config.HelpKV{
			Key:         ReplicationWorkersWait,
			Description: `maximum sleep duration between objects to slow down batch replication operation` + defaultHelpPostfix(ReplicationWorkersWait),
			Optional:    true,
			Type:        "duration",
		},
		config.HelpKV{
			Key:         KeyRotationWorkersWait,
			Description: `maximum sleep duration between objects to slow down batch keyrotation operation` + defaultHelpPostfix(KeyRotationWorkersWait),
			Optional:    true,
			Type:        "duration",
		},
		config.HelpKV{
			Key:         ExpirationWorkersWait,
			Description: "maximum sleep duration between objects to slow down batch expiration operation" + defaultHelpPostfix(ExpirationWorkersWait),
			Optional:    true,
			Type:        "duration",
		},
		config.HelpKV{
			Key:         MaxJobs,
			Description: "maximum number of batch jobs that may run concurrently on this server" + defaultHelpPostfix(MaxJobs),
			Optional:    true,
			Type:        "number",
		},
		config.HelpKV{
			Key:         ReplicationMaxJobs,
			Description: "maximum number of concurrent batch replication jobs (0 means unlimited)" + defaultHelpPostfix(ReplicationMaxJobs),
			Optional:    true,
			Type:        "number",
		},
		config.HelpKV{
			Key:         KeyRotationMaxJobs,
			Description: "maximum number of concurrent batch keyrotation jobs (0 means unlimited)" + defaultHelpPostfix(KeyRotationMaxJobs),
			Optional:    true,
			Type:        "number",
		},
		config.HelpKV{
			Key:         ExpirationMaxJobs,
			Description: "maximum number of concurrent batch expiration jobs (0 means unlimited)" + defaultHelpPostfix(ExpirationMaxJobs),
			Optional:    true,
			Type:        "number",
		},
		config.HelpKV{
			Key:         QueueCapacity,
			Description: "maximum number of jobs allowed to wait for admission before new submissions are rejected" + defaultHelpPostfix(QueueCapacity),
			Optional:    true,
			Type:        "number",
		},
		config.HelpKV{
			Key:         CPUWatermark,
			Description: "CPU usage percentage (load1/cores) above which new batch jobs are queued instead of started (0 disables)" + defaultHelpPostfix(CPUWatermark),
			Optional:    true,
			Type:        "number",
		},
		config.HelpKV{
			Key:         MemWatermark,
			Description: "used memory percentage above which new batch jobs are queued instead of started (0 disables)" + defaultHelpPostfix(MemWatermark),
			Optional:    true,
			Type:        "number",
		},
		config.HelpKV{
			Key:         DriveWatermark,
			Description: "used drive space percentage above which new batch jobs are queued instead of started (0 disables)" + defaultHelpPostfix(DriveWatermark),
			Optional:    true,
			Type:        "number",
		},
		config.HelpKV{
			Key:         ResourceThrottleWait,
			Description: "extra per-object sleep applied to running batch jobs while a resource watermark is breached" + defaultHelpPostfix(ResourceThrottleWait),
			Optional:    true,
			Type:        "duration",
		},
		config.HelpKV{
			Key:         ReplicationPriority,
			Description: "default priority assigned to batch replication jobs without an explicit priority field" + defaultHelpPostfix(ReplicationPriority),
			Optional:    true,
			Type:        "string",
		},
		config.HelpKV{
			Key:         KeyRotationPriority,
			Description: "default priority assigned to batch keyrotation jobs without an explicit priority field" + defaultHelpPostfix(KeyRotationPriority),
			Optional:    true,
			Type:        "string",
		},
		config.HelpKV{
			Key:         ExpirationPriority,
			Description: "default priority assigned to batch expiration jobs without an explicit priority field" + defaultHelpPostfix(ExpirationPriority),
			Optional:    true,
			Type:        "string",
		},
	}
)
