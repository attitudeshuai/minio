// Copyright (c) 2015-2021 MinIO, Inc.
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
	"strconv"
	"time"

	jsoniter "github.com/json-iterator/go"
	"github.com/minio/madmin-go/v3"
	"github.com/minio/minio/internal/config/storageclass"
	"github.com/minio/mux"
	"github.com/minio/pkg/v3/policy"
)

var (
	// error returned when transition failure handling is disabled.
	errTierFailureHandlingDisabled = AdminError{
		Code:       "XMinioAdminTierFailureHandlingDisabled",
		Message:    "Transition failure handling is disabled; set ilm transition_failure_handling=on first",
		StatusCode: http.StatusPreconditionFailed,
	}
	// error returned for invalid failure list filters.
	errTierFailureInvalidFilter = AdminError{
		Code:       "XMinioAdminTierFailureInvalidFilter",
		Message:    "Invalid status filter; allowed values are 'pending' and 'exhausted'",
		StatusCode: http.StatusBadRequest,
	}

	// error returned when remote tier already exists
	errTierAlreadyExists = AdminError{
		Code:       "XMinioAdminTierAlreadyExists",
		Message:    "Specified remote tier already exists",
		StatusCode: http.StatusConflict,
	}
	// error returned when remote tier is not found
	errTierNotFound = AdminError{
		Code:       "XMinioAdminTierNotFound",
		Message:    "Specified remote tier was not found",
		StatusCode: http.StatusNotFound,
	}
	// error returned when remote tier name is not in uppercase
	errTierNameNotUppercase = AdminError{
		Code:       "XMinioAdminTierNameNotUpperCase",
		Message:    "Tier name must be in uppercase",
		StatusCode: http.StatusBadRequest,
	}
	// error returned when remote tier bucket is not found
	errTierBucketNotFound = AdminError{
		Code:       "XMinioAdminTierBucketNotFound",
		Message:    "Remote tier bucket not found",
		StatusCode: http.StatusBadRequest,
	}
	// error returned when remote tier credentials are invalid.
	errTierInvalidCredentials = AdminError{
		Code:       "XMinioAdminTierInvalidCredentials",
		Message:    "Invalid remote tier credentials",
		StatusCode: http.StatusBadRequest,
	}
	// error returned when reserved internal names are used.
	errTierReservedName = AdminError{
		Code:       "XMinioAdminTierReserved",
		Message:    "Cannot use reserved tier name",
		StatusCode: http.StatusBadRequest,
	}
)

func (api adminAPIHandlers) AddTierHandler(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	objAPI, cred := validateAdminReq(ctx, w, r, policy.SetTierAction)
	if objAPI == nil {
		return
	}

	password := cred.SecretKey
	reqBytes, err := madmin.DecryptData(password, io.LimitReader(r.Body, r.ContentLength))
	if err != nil {
		writeErrorResponseJSON(ctx, w, errorCodes.ToAPIErrWithErr(ErrAdminConfigBadJSON, err), r.URL)
		return
	}

	var cfg madmin.TierConfig
	json := jsoniter.ConfigCompatibleWithStandardLibrary
	if err := json.Unmarshal(reqBytes, &cfg); err != nil {
		writeErrorResponseJSON(ctx, w, toAdminAPIErr(ctx, err), r.URL)
		return
	}

	var ignoreInUse bool
	if forceStr := r.Form.Get("force"); forceStr != "" {
		ignoreInUse, _ = strconv.ParseBool(forceStr)
	}

	// Disallow remote tiers with internal storage class names
	switch cfg.Name {
	case storageclass.STANDARD, storageclass.RRS:
		writeErrorResponseJSON(ctx, w, toAdminAPIErr(ctx, errTierReservedName), r.URL)
		return
	}

	// Refresh from the disk in case we had missed notifications about edits from peers.
	if err := globalTierConfigMgr.Reload(ctx, objAPI); err != nil {
		writeErrorResponseJSON(ctx, w, toAdminAPIErr(ctx, err), r.URL)
		return
	}

	err = globalTierConfigMgr.Add(ctx, cfg, ignoreInUse)
	if err != nil {
		writeErrorResponseJSON(ctx, w, toAdminAPIErr(ctx, err), r.URL)
		return
	}

	err = globalTierConfigMgr.Save(ctx, objAPI)
	if err != nil {
		writeErrorResponseJSON(ctx, w, toAdminAPIErr(ctx, err), r.URL)
		return
	}
	globalNotificationSys.LoadTransitionTierConfig(ctx)

	writeSuccessNoContent(w)
}

func (api adminAPIHandlers) ListTierHandler(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	objAPI, _ := validateAdminReq(ctx, w, r, policy.ListTierAction)
	if objAPI == nil {
		return
	}

	tiers := globalTierConfigMgr.ListTiers()
	data, err := json.Marshal(tiers)
	if err != nil {
		writeErrorResponseJSON(ctx, w, toAdminAPIErr(ctx, err), r.URL)
		return
	}

	w.Header().Set(tierCfgRefreshAtHdr, globalTierConfigMgr.refreshedAt().String())
	writeSuccessResponseJSON(w, data)
}

func (api adminAPIHandlers) EditTierHandler(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	objAPI, cred := validateAdminReq(ctx, w, r, policy.SetTierAction)
	if objAPI == nil {
		return
	}
	vars := mux.Vars(r)
	scName := vars["tier"]

	password := cred.SecretKey
	reqBytes, err := madmin.DecryptData(password, io.LimitReader(r.Body, r.ContentLength))
	if err != nil {
		writeErrorResponseJSON(ctx, w, errorCodes.ToAPIErrWithErr(ErrAdminConfigBadJSON, err), r.URL)
		return
	}

	var creds madmin.TierCreds
	json := jsoniter.ConfigCompatibleWithStandardLibrary
	if err := json.Unmarshal(reqBytes, &creds); err != nil {
		writeErrorResponseJSON(ctx, w, toAdminAPIErr(ctx, err), r.URL)
		return
	}

	// Refresh from the disk in case we had missed notifications about edits from peers.
	if err := globalTierConfigMgr.Reload(ctx, objAPI); err != nil {
		writeErrorResponseJSON(ctx, w, toAdminAPIErr(ctx, err), r.URL)
		return
	}

	if err := globalTierConfigMgr.Edit(ctx, scName, creds); err != nil {
		writeErrorResponseJSON(ctx, w, toAdminAPIErr(ctx, err), r.URL)
		return
	}

	if err := globalTierConfigMgr.Save(ctx, objAPI); err != nil {
		writeErrorResponseJSON(ctx, w, toAdminAPIErr(ctx, err), r.URL)
		return
	}
	globalNotificationSys.LoadTransitionTierConfig(ctx)

	writeSuccessNoContent(w)
}

func (api adminAPIHandlers) RemoveTierHandler(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	objAPI, _ := validateAdminReq(ctx, w, r, policy.SetTierAction)
	if objAPI == nil {
		return
	}

	vars := mux.Vars(r)
	tier := vars["tier"]
	force := r.Form.Get("force") == "true"

	if err := globalTierConfigMgr.Reload(ctx, objAPI); err != nil {
		writeErrorResponseJSON(ctx, w, toAdminAPIErr(ctx, err), r.URL)
		return
	}

	if err := globalTierConfigMgr.Remove(ctx, tier, force); err != nil {
		writeErrorResponseJSON(ctx, w, toAdminAPIErr(ctx, err), r.URL)
		return
	}

	if err := globalTierConfigMgr.Save(ctx, objAPI); err != nil {
		writeErrorResponseJSON(ctx, w, toAdminAPIErr(ctx, err), r.URL)
		return
	}
	globalNotificationSys.LoadTransitionTierConfig(ctx)

	writeSuccessNoContent(w)
}

func (api adminAPIHandlers) VerifyTierHandler(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	objAPI, _ := validateAdminReq(ctx, w, r, policy.ListTierAction)
	if objAPI == nil {
		return
	}

	vars := mux.Vars(r)
	tier := vars["tier"]
	if err := globalTierConfigMgr.Verify(ctx, tier); err != nil {
		writeErrorResponseJSON(ctx, w, toAdminAPIErr(ctx, err), r.URL)
		return
	}

	writeSuccessNoContent(w)
}

func (api adminAPIHandlers) TierStatsHandler(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	objAPI, _ := validateAdminReq(ctx, w, r, policy.ListTierAction)
	if objAPI == nil {
		return
	}

	dui, err := loadDataUsageFromBackend(ctx, objAPI)
	if err != nil {
		writeErrorResponseJSON(ctx, w, toAdminAPIErr(ctx, err), r.URL)
		return
	}

	tierStats := dui.tierStats()
	dailyStats := globalNotificationSys.GetLastDayTierStats(ctx)
	tierStats = dailyStats.addToTierInfo(tierStats)

	data, err := json.Marshal(tierStats)
	if err != nil {
		writeErrorResponseJSON(ctx, w, toAdminAPIErr(ctx, err), r.URL)
		return
	}
	writeSuccessResponseJSON(w, data)
}

// tierFailureInfo is one entry in the per-tier failed transition list.
type tierFailureInfo struct {
	ID            string    `json:"id"`
	Bucket        string    `json:"bucket"`
	Object        string    `json:"object"`
	VersionID     string    `json:"versionId"`
	Tier          string    `json:"tier"`
	Size          int64     `json:"size"`
	ETag          string    `json:"etag"`
	LastModified  time.Time `json:"lastModified"`
	Noncurrent    bool      `json:"noncurrent"`
	Category      string    `json:"errorCategory"`
	LastErr       string    `json:"lastError"`
	Attempts      int       `json:"attempts"`
	FirstFailedAt time.Time `json:"firstFailedAt"`
	LastFailedAt  time.Time `json:"lastFailedAt"`
	NextRetryAt   time.Time `json:"nextRetryAt"`
	Status        string    `json:"status"`
}

func newTierFailureInfo(rec transitionFailureRecord) tierFailureInfo {
	return tierFailureInfo{
		ID:            rec.ID,
		Bucket:        rec.Bucket,
		Object:        rec.Object,
		VersionID:     rec.VersionID,
		Tier:          rec.Tier,
		Size:          rec.Size,
		ETag:          rec.ETag,
		LastModified:  rec.ModTime,
		Noncurrent:    rec.Noncurrent,
		Category:      string(rec.Category),
		LastErr:       rec.LastErr,
		Attempts:      rec.Attempts,
		FirstFailedAt: rec.FirstFailedAt,
		LastFailedAt:  rec.LastFailedAt,
		NextRetryAt:   rec.NextRetryAt,
		Status:        string(rec.Status),
	}
}

// tierStatusInfo describes the pause/health state of one remote tier.
type tierStatusInfo struct {
	Tier                string    `json:"tier"`
	Paused              bool      `json:"paused"`
	Manual              bool      `json:"manual"`
	ConsecutiveFailures int       `json:"consecutiveFailures"`
	PausedAt            time.Time `json:"pausedAt,omitempty"`
	LastFailureAt       time.Time `json:"lastFailureAt,omitempty"`
	LastErr             string    `json:"lastError,omitempty"`
	ResumedAt           time.Time `json:"resumedAt,omitempty"`
	Version             uint64    `json:"version"`
	PendingFailures     int       `json:"pendingFailures"`
	ExhaustedFailures   int       `json:"exhaustedFailures"`
}

// ListTierFailuresHandler returns the archived failure list, optionally
// filtered by tier, bucket and status.
func (api adminAPIHandlers) ListTierFailuresHandler(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	objAPI, _ := validateAdminReq(ctx, w, r, policy.ListTierAction)
	if objAPI == nil {
		return
	}

	tierName := r.Form.Get("tier")
	bucket := r.Form.Get("bucket")
	switch transitionFailureStatus(r.Form.Get("status")) {
	case "", transitionFailurePending, transitionFailureExhausted:
	default:
		writeErrorResponseJSON(ctx, w, toAdminAPIErr(ctx, errTierFailureInvalidFilter), r.URL)
		return
	}
	status := transitionFailureStatus(r.Form.Get("status"))

	if globalTransitionFailureJournal == nil {
		writeSuccessResponseJSON(w, []byte("[]"))
		return
	}

	infos, err := listTierFailures(ctx, globalTransitionFailureJournal, tierName, bucket, status)
	if err != nil {
		writeErrorResponseJSON(ctx, w, toAdminAPIErr(ctx, err), r.URL)
		return
	}
	data, err := json.Marshal(infos)
	if err != nil {
		writeErrorResponseJSON(ctx, w, toAdminAPIErr(ctx, err), r.URL)
		return
	}
	writeSuccessResponseJSON(w, data)
}

// ClearTierFailuresHandler removes failure records. By default only exhausted
// records are removed; all=true is required to remove pending ones too.
func (api adminAPIHandlers) ClearTierFailuresHandler(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	objAPI, _ := validateAdminReq(ctx, w, r, policy.SetTierAction)
	if objAPI == nil {
		return
	}

	tierName := r.Form.Get("tier")
	all := r.Form.Get("all") == "true"
	if globalTransitionFailureJournal == nil {
		writeSuccessResponseJSON(w, []byte(`{"cleared":0}`))
		return
	}
	removed, err := clearTierFailures(ctx, globalTransitionFailureJournal, tierName, all)
	if err != nil {
		writeErrorResponseJSON(ctx, w, toAdminAPIErr(ctx, err), r.URL)
		return
	}
	writeSuccessResponseJSON(w, []byte(`{"cleared":`+strconv.Itoa(removed)+`}`))
}

// TierStatusHandler returns the cluster-wide pause state of remote tiers.
func (api adminAPIHandlers) TierStatusHandler(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	objAPI, _ := validateAdminReq(ctx, w, r, policy.ListTierAction)
	if objAPI == nil {
		return
	}

	if globalTransitionTierHealth == nil {
		writeSuccessResponseJSON(w, []byte("[]"))
		return
	}

	infos, err := listTierStatus(ctx, globalTransitionTierHealth, globalTransitionFailureJournal, r.Form.Get("tier"))
	if err != nil {
		writeErrorResponseJSON(ctx, w, toAdminAPIErr(ctx, err), r.URL)
		return
	}
	data, err := json.Marshal(infos)
	if err != nil {
		writeErrorResponseJSON(ctx, w, toAdminAPIErr(ctx, err), r.URL)
		return
	}
	writeSuccessResponseJSON(w, data)
}

// PauseTierHandler manually pauses transitions to a tier.
func (api adminAPIHandlers) PauseTierHandler(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	objAPI, _ := validateAdminReq(ctx, w, r, policy.SetTierAction)
	if objAPI == nil {
		return
	}
	tierName := mux.Vars(r)["tier"]

	if globalTransitionTierHealth == nil {
		writeErrorResponseJSON(ctx, w, toAdminAPIErr(ctx, errTierFailureHandlingDisabled), r.URL)
		return
	}
	if err := pauseTransitionTier(ctx, objAPI, globalTransitionTierHealth, tierName, r.Form.Get("reason")); err != nil {
		writeErrorResponseJSON(ctx, w, toAdminAPIErr(ctx, err), r.URL)
		return
	}
	writeSuccessNoContent(w)
}

// ResumeTierHandler manually resumes transitions to a tier.
func (api adminAPIHandlers) ResumeTierHandler(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	objAPI, _ := validateAdminReq(ctx, w, r, policy.SetTierAction)
	if objAPI == nil {
		return
	}
	tierName := mux.Vars(r)["tier"]

	if globalTransitionTierHealth == nil {
		writeErrorResponseJSON(ctx, w, toAdminAPIErr(ctx, errTierFailureHandlingDisabled), r.URL)
		return
	}
	if err := resumeTransitionTier(ctx, objAPI, globalTransitionTierHealth, tierName); err != nil {
		writeErrorResponseJSON(ctx, w, toAdminAPIErr(ctx, err), r.URL)
		return
	}
	writeSuccessNoContent(w)
}
