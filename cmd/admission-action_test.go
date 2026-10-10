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
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/minio/minio/internal/admission"
	"github.com/minio/minio/internal/auth"
	"github.com/minio/pkg/v3/policy"
)

func TestClassifyAction(t *testing.T) {
	listActions := []policy.Action{
		policy.ListAllMyBucketsAction,
		policy.ListBucketAction,
		policy.ListBucketVersionsAction,
		policy.ListBucketMultipartUploadsAction,
		policy.ListMultipartUploadPartsAction,
	}
	readActions := []policy.Action{
		policy.GetObjectAction,
		policy.GetObjectAttributesAction,
		policy.HeadBucketAction,
		policy.GetBucketLocationAction,
		policy.GetBucketPolicyAction,
		policy.GetBucketLifecycleAction,
		policy.GetBucketEncryptionAction,
		policy.GetBucketVersioningAction,
		policy.GetObjectTaggingAction,
		policy.GetObjectRetentionAction,
		policy.ListenBucketNotificationAction,
		policy.GetObjectVersionAction,
	}
	writeActions := []policy.Action{
		policy.PutObjectAction,
		policy.DeleteObjectAction,
		policy.CreateBucketAction,
		policy.DeleteBucketAction,
		policy.AbortMultipartUploadAction,
		policy.PutBucketPolicyAction,
		policy.PutBucketEncryptionAction,
		policy.PutObjectTaggingAction,
		policy.DeleteObjectTaggingAction,
		policy.RestoreObjectAction,
		policy.PutObjectRetentionAction,
		policy.PutBucketObjectLockConfigurationAction,
	}

	for _, a := range listActions {
		if got := classifyAction(a); got != admission.ClassList {
			t.Errorf("%s: want list, got %s", a, got)
		}
	}
	for _, a := range readActions {
		if got := classifyAction(a); got != admission.ClassRead {
			t.Errorf("%s: want read, got %s", a, got)
		}
	}
	for _, a := range writeActions {
		if got := classifyAction(a); got != admission.ClassWrite {
			t.Errorf("%s: want write, got %s", a, got)
		}
	}
}

func TestRequestActionClassMethodFallback(t *testing.T) {
	unknown := policy.Action("s3:SomeFutureExtensionAction")
	cases := []struct {
		method string
		want   admission.ActionClass
	}{
		{http.MethodGet, admission.ClassRead},
		{http.MethodHead, admission.ClassRead},
		{http.MethodPut, admission.ClassWrite},
		{http.MethodPost, admission.ClassWrite},
		{http.MethodDelete, admission.ClassWrite},
		{http.MethodPatch, admission.ClassWrite},
	}
	for _, c := range cases {
		r := httptest.NewRequest(c.method, "/", nil)
		if got := requestActionClass(r, unknown); got != c.want {
			t.Errorf("%s: want %s, got %s", c.method, c.want, got)
		}
	}

	// Explicit mapping wins over the method.
	r := httptest.NewRequest(http.MethodDelete, "/", nil)
	if got := requestActionClass(r, policy.ListBucketAction); got != admission.ClassList {
		t.Fatalf("explicit list mapping must win, got %s", got)
	}
}

func TestAdmissionCaller(t *testing.T) {
	// Static user: own access key.
	static := auth.Credentials{AccessKey: "alice"}
	if got := admissionCaller(static); got != "alice" {
		t.Fatalf("static user: got %q", got)
	}

	// Service account: non-temp, uses its own access key even with parent.
	sa := auth.Credentials{
		AccessKey:  "SAKEY1234",
		ParentUser: "alice",
		Claims:     map[string]any{iamPolicyClaimNameSA(): "readwrite"},
	}
	if sa.IsServiceAccount() != true {
		t.Fatal("test setup: expected service account")
	}
	if got := admissionCaller(sa); got != "SAKEY1234" {
		t.Fatalf("service account: got %q", got)
	}

	// STS temporary credential: merged into parent user.
	sts := auth.Credentials{
		AccessKey:    "TEMPKEY",
		ParentUser:   "alice",
		SessionToken: "token",
		Expiration:   time.Now().Add(time.Hour),
	}
	if sts.IsTemp() != true {
		t.Fatal("test setup: expected temp credential")
	}
	if got := admissionCaller(sts); got != "alice" {
		t.Fatalf("sts credential: got %q", got)
	}

	// Anonymous: no caller dimension.
	if got := admissionCaller(auth.Credentials{}); got != "" {
		t.Fatalf("anonymous: got %q", got)
	}
}
