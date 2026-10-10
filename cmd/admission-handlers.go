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
	"encoding/json"
	"errors"
	"net/http"

	"github.com/minio/minio/internal/admission"
	"github.com/minio/pkg/v3/policy"
)

// setRequestAdmissionReq is the body of PUT set-request-admission. At least
// one of total/reads/writes/lists must be present; sending an empty body
// removes all quotas (invalid as a standalone rule).
type setRequestAdmissionReq struct {
	Total  *admission.ClassQuota `json:"total,omitempty"`
	Reads  *admission.ClassQuota `json:"reads,omitempty"`
	Writes *admission.ClassQuota `json:"writes,omitempty"`
	Lists  *admission.ClassQuota `json:"lists,omitempty"`
}

// getRequestAdmissionResp is the body of GET get-request-admission.
type getRequestAdmissionResp struct {
	Rules admission.Rules       `json:"rules"`
	Stats []admission.RuleUsage `json:"stats"`
}

func parseAdmissionScopeName(r *http.Request) (admission.Scope, string, bool) {
	scope := admission.Scope(r.FormValue("scope"))
	name := r.FormValue("name")
	if scope != admission.ScopeBucket && scope != admission.ScopeCaller {
		return "", "", false
	}
	if name == "" {
		return "", "", false
	}
	return scope, name, true
}

// SetRequestAdmissionHandler - PUT creates/replaces one admission rule.
func (a adminAPIHandlers) SetRequestAdmissionHandler(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	objectAPI, _ := validateAdminReq(ctx, w, r, policy.ConfigUpdateAdminAction)
	if objectAPI == nil {
		return
	}

	scope, name, ok := parseAdmissionScopeName(r)
	if !ok {
		writeErrorResponseJSON(ctx, w, errorCodes.ToAPIErr(ErrInvalidRequest), r.URL)
		return
	}

	data, err := readAdmissionBody(r.Body)
	if err != nil {
		writeErrorResponseJSON(ctx, w, errorCodes.ToAPIErr(ErrInvalidRequest), r.URL)
		return
	}
	if len(data) > admissionRulesMaxBodySize {
		writeErrorResponseJSON(ctx, w, errorCodes.ToAPIErr(ErrEntityTooLarge), r.URL)
		return
	}

	var req setRequestAdmissionReq
	if len(data) > 0 {
		if err := json.Unmarshal(data, &req); err != nil {
			writeErrorResponseJSON(ctx, w, errorCodes.ToAPIErr(ErrAdminConfigBadJSON), r.URL)
			return
		}
	}

	rule := admission.Rule{
		Scope:  scope,
		Name:   name,
		Total:  req.Total,
		Reads:  req.Reads,
		Writes: req.Writes,
		Lists:  req.Lists,
	}
	if err := globalAdmissionSys.upsertRule(ctx, objectAPI, rule); err != nil {
		writeErrorResponseJSON(ctx, w, toAdminAPIErr(ctx, err), r.URL)
		return
	}

	writeSuccessResponseHeadersOnly(w)
}

// GetRequestAdmissionHandler - GET lists rules and live counters. Optional
// scope/name query parameters filter the result.
func (a adminAPIHandlers) GetRequestAdmissionHandler(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	objectAPI, _ := validateAdminReq(ctx, w, r, policy.ConfigUpdateAdminAction)
	if objectAPI == nil {
		return
	}

	rules := globalAdmissionSys.currentRules()
	stats := globalAdmissionSys.mgr.SnapshotStats()

	if scopeParam := r.FormValue("scope"); scopeParam != "" {
		scope := admission.Scope(scopeParam)
		if scope != admission.ScopeBucket && scope != admission.ScopeCaller {
			writeErrorResponseJSON(ctx, w, errorCodes.ToAPIErr(ErrInvalidRequest), r.URL)
			return
		}
		name := r.FormValue("name")
		filtered := rules[:0]
		for _, ru := range rules {
			if ru.Scope != scope {
				continue
			}
			if name != "" && ru.Name != name {
				continue
			}
			filtered = append(filtered, ru)
		}
		rules = filtered

		filteredStats := stats[:0]
		for _, su := range stats {
			if su.Scope != scope {
				continue
			}
			if name != "" && su.Name != name {
				continue
			}
			filteredStats = append(filteredStats, su)
		}
		stats = filteredStats
	}

	if rules == nil {
		rules = admission.Rules{}
	}
	if stats == nil {
		stats = []admission.RuleUsage{}
	}
	resp := getRequestAdmissionResp{Rules: rules, Stats: stats}
	data, err := json.Marshal(resp)
	if err != nil {
		writeErrorResponseJSON(ctx, w, toAdminAPIErr(ctx, err), r.URL)
		return
	}
	writeSuccessResponseJSON(w, data)
}

// DeleteRequestAdmissionHandler - DELETE removes one admission rule.
func (a adminAPIHandlers) DeleteRequestAdmissionHandler(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	objectAPI, _ := validateAdminReq(ctx, w, r, policy.ConfigUpdateAdminAction)
	if objectAPI == nil {
		return
	}

	scope, name, ok := parseAdmissionScopeName(r)
	if !ok {
		writeErrorResponseJSON(ctx, w, errorCodes.ToAPIErr(ErrInvalidRequest), r.URL)
		return
	}

	if err := globalAdmissionSys.deleteRule(ctx, objectAPI, scope, name); err != nil {
		if errors.Is(err, errAdmissionRuleNotFound) {
			writeErrorResponseJSON(ctx, w, errorCodes.ToAPIErr(ErrAdminNoSuchConfigTarget), r.URL)
			return
		}
		writeErrorResponseJSON(ctx, w, toAdminAPIErr(ctx, err), r.URL)
		return
	}

	writeSuccessResponseHeadersOnly(w)
}
