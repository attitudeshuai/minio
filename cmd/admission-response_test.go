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
	"io"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	xhttp "github.com/minio/minio/internal/http"
	"github.com/minio/minio/internal/logger"
)

func admissionCtx() context.Context {
	return logger.SetReqInfo(context.Background(), &logger.ReqInfo{})
}

func TestWriteAdmissionRateLimitedResponse(t *testing.T) {
	ctx := admissionCtx()
	reqInfo := logger.GetReqInfo(ctx)
	reqInfo.AdmissionRateLimited = true
	reqInfo.AdmissionRetryAfter = 2500 * time.Millisecond
	reqInfo.AdmissionLimit = 10
	reqInfo.AdmissionRemaining = 0

	rec := httptest.NewRecorder()
	writeErrorResponse(ctx, rec, errorCodes.ToAPIErr(ErrAdmissionRateLimited), &url.URL{Path: "/bucket/object"})

	resp := rec.Result()
	if resp.StatusCode != 429 {
		t.Fatalf("status: want 429, got %d", resp.StatusCode)
	}
	if got := resp.Header.Get(xhttp.RetryAfter); got != "3" {
		t.Fatalf("Retry-After: want 3, got %q", got)
	}
	if got := resp.Header.Get(xhttp.RateLimitLimit); got != "10" {
		t.Fatalf("X-RateLimit-Limit: want 10, got %q", got)
	}
	if got := resp.Header.Get(xhttp.RateLimitRemaining); got != "0" {
		t.Fatalf("X-RateLimit-Remaining: want 0, got %q", got)
	}
	body, _ := io.ReadAll(resp.Body)
	if !strings.Contains(string(body), "<Code>RequestRateLimited</Code>") {
		t.Fatalf("body must carry RequestRateLimited code: %s", body)
	}
}

func TestWriteAdmissionRateLimitedMinRetryOneSecond(t *testing.T) {
	ctx := admissionCtx()
	reqInfo := logger.GetReqInfo(ctx)
	reqInfo.AdmissionRateLimited = true
	reqInfo.AdmissionRetryAfter = 10 * time.Millisecond // rounds below 1s
	reqInfo.AdmissionLimit = 1

	rec := httptest.NewRecorder()
	writeErrorResponse(ctx, rec, errorCodes.ToAPIErr(ErrAdmissionRateLimited), &url.URL{Path: "/"})
	if got := rec.Result().Header.Get(xhttp.RetryAfter); got != "1" {
		t.Fatalf("Retry-After minimum must be 1 second, got %q", got)
	}
}

func TestWriteAdmissionWithoutDecisionKeepsGenericRetry(t *testing.T) {
	// No decision on ReqInfo: generic 60s Retry-After, no rate limit headers.
	ctx := admissionCtx()
	rec := httptest.NewRecorder()
	writeErrorResponse(ctx, rec, errorCodes.ToAPIErr(ErrAdmissionRateLimited), &url.URL{Path: "/"})
	if got := rec.Result().Header.Get(xhttp.RetryAfter); got != "60" {
		t.Fatalf("generic Retry-After want 60, got %q", got)
	}
	if got := rec.Result().Header.Get(xhttp.RateLimitLimit); got != "" {
		t.Fatalf("no admission limit header expected, got %q", got)
	}
}

func TestWriteTooManyRequestsUnaffected(t *testing.T) {
	ctx := admissionCtx()
	rec := httptest.NewRecorder()
	writeErrorResponse(ctx, rec, errorCodes.ToAPIErr(ErrTooManyRequests), &url.URL{Path: "/"})
	resp := rec.Result()
	if resp.StatusCode != 429 {
		t.Fatalf("status: %d", resp.StatusCode)
	}
	body, _ := io.ReadAll(resp.Body)
	if !strings.Contains(string(body), "<Code>TooManyRequests</Code>") {
		t.Fatalf("global gate error must keep its own code: %s", body)
	}
	if got := resp.Header.Get(xhttp.RateLimitLimit); got != "" {
		t.Fatalf("writeErrorResponse must not set rate limit headers for global gate, got %q", got)
	}
}

func TestAdmissionErrorCodeString(t *testing.T) {
	if got := ErrAdmissionRateLimited.String(); got != "AdmissionRateLimited" {
		t.Fatalf("stringer: got %q", got)
	}
	apiErr := errorCodes.ToAPIErr(ErrAdmissionRateLimited)
	if apiErr.Code != "RequestRateLimited" {
		t.Fatalf("code: got %q", apiErr.Code)
	}
}
