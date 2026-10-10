// Copyright (c) 2015-2024 MinIO, Inc.
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
	"encoding/json"
	"io"
	"net/http"

	"github.com/minio/mux"
	"github.com/minio/pkg/v3/policy"
)

// GetBucketScannerConfigHandler returns per-bucket scanner status:
// progress position, processed object count, weight/budget and paused
// state. A specific bucket can be requested, otherwise every bucket with
// configuration or recorded progress is returned.
func (a adminAPIHandlers) GetBucketScannerConfigHandler(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	objectAPI, _ := validateAdminReq(ctx, w, r, policy.HealAdminAction)
	if objectAPI == nil {
		return
	}

	bucket := pathClean(mux.Vars(r)["bucket"])
	if bucket != "" {
		if _, err := objectAPI.GetBucketInfo(ctx, bucket, BucketOptions{}); err != nil {
			writeErrorResponseJSON(ctx, w, toAPIError(ctx, err), r.URL)
			return
		}
	}

	status, err := AdminBucketScanStatus(ctx, objectAPI, bucket)
	if err != nil {
		writeErrorResponseJSON(ctx, w, toAdminAPIErr(ctx, err), r.URL)
		return
	}
	data, err := json.Marshal(status)
	if err != nil {
		writeErrorResponseJSON(ctx, w, toAdminAPIErr(ctx, err), r.URL)
		return
	}
	writeSuccessResponseJSON(w, data)
}

// PutBucketScannerConfigHandler stores per-bucket scanner weight/budget.
// Sending an empty body (or all-zero configuration) resets the bucket to
// default scheduling.
func (a adminAPIHandlers) PutBucketScannerConfigHandler(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	objectAPI, _ := validateAdminReq(ctx, w, r, policy.HealAdminAction)
	if objectAPI == nil {
		return
	}

	bucket := pathClean(mux.Vars(r)["bucket"])
	if bucket == "" {
		writeErrorResponseJSON(ctx, w, errorCodes.ToAPIErr(ErrInvalidBucketName), r.URL)
		return
	}
	if _, err := objectAPI.GetBucketInfo(ctx, bucket, BucketOptions{}); err != nil {
		writeErrorResponseJSON(ctx, w, toAPIError(ctx, err), r.URL)
		return
	}

	data, err := io.ReadAll(r.Body)
	if err != nil {
		writeErrorResponseJSON(ctx, w, errorCodes.ToAPIErr(ErrInvalidRequest), r.URL)
		return
	}
	var cfg BucketScanConfig
	if len(data) > 0 {
		if err := json.Unmarshal(data, &cfg); err != nil {
			writeErrorResponseJSON(ctx, w, errorCodes.ToAPIErrWithErr(ErrInvalidRequest, err), r.URL)
			return
		}
	}
	// Preserve a previously set paused flag; pausing is managed through
	// the dedicated pause/resume endpoints.
	if cur, err := AdminBucketScanStatus(ctx, objectAPI, bucket); err == nil {
		for _, st := range cur {
			if st.Bucket == bucket {
				cfg.Paused = st.Paused
			}
		}
	}
	if err := AdminSetBucketScanConfig(ctx, objectAPI, bucket, cfg); err != nil {
		writeErrorResponseJSON(ctx, w, errorCodes.ToAPIErrWithErr(ErrAdminConfigBadJSON, err), r.URL)
		return
	}
	writeSuccessResponseHeadersOnly(w)
}

// PauseBucketScannerHandler pauses scanning of a bucket.
func (a adminAPIHandlers) PauseBucketScannerHandler(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	objectAPI, _ := validateAdminReq(ctx, w, r, policy.HealAdminAction)
	if objectAPI == nil {
		return
	}

	bucket := pathClean(mux.Vars(r)["bucket"])
	if bucket == "" {
		writeErrorResponseJSON(ctx, w, errorCodes.ToAPIErr(ErrInvalidBucketName), r.URL)
		return
	}
	if _, err := objectAPI.GetBucketInfo(ctx, bucket, BucketOptions{}); err != nil {
		writeErrorResponseJSON(ctx, w, toAPIError(ctx, err), r.URL)
		return
	}
	if err := AdminPauseBucketScan(ctx, objectAPI, bucket); err != nil {
		writeErrorResponseJSON(ctx, w, toAdminAPIErr(ctx, err), r.URL)
		return
	}
	writeSuccessResponseHeadersOnly(w)
}

// ResumeBucketScannerHandler resumes scanning of a paused bucket. The
// bucket continues from its saved position within the current cycle.
func (a adminAPIHandlers) ResumeBucketScannerHandler(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	objectAPI, _ := validateAdminReq(ctx, w, r, policy.HealAdminAction)
	if objectAPI == nil {
		return
	}

	bucket := pathClean(mux.Vars(r)["bucket"])
	if bucket == "" {
		writeErrorResponseJSON(ctx, w, errorCodes.ToAPIErr(ErrInvalidBucketName), r.URL)
		return
	}
	if _, err := objectAPI.GetBucketInfo(ctx, bucket, BucketOptions{}); err != nil {
		writeErrorResponseJSON(ctx, w, toAPIError(ctx, err), r.URL)
		return
	}
	if err := AdminResumeBucketScan(ctx, objectAPI, bucket); err != nil {
		writeErrorResponseJSON(ctx, w, toAdminAPIErr(ctx, err), r.URL)
		return
	}
	writeSuccessResponseHeadersOnly(w)
}
