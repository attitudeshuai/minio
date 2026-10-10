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

	"github.com/minio/minio/internal/admission"
	"github.com/minio/minio/internal/auth"
	"github.com/minio/pkg/v3/policy"
)

// classifyAction maps an authenticated S3 policy action to an admission
// rate-limit class. Actions not covered by the explicit tables return "" so
// callers can fall back to HTTP method based classification.
func classifyAction(a policy.Action) admission.ActionClass {
	switch a {
	case policy.ListAllMyBucketsAction,
		policy.ListBucketAction,
		policy.ListBucketVersionsAction,
		policy.ListBucketMultipartUploadsAction,
		policy.ListMultipartUploadPartsAction:
		return admission.ClassList

	case policy.GetObjectAction,
		policy.GetObjectAttributesAction,
		policy.GetObjectVersionAction,
		policy.GetObjectVersionAttributesAction,
		policy.GetObjectVersionForReplicationAction,
		policy.HeadBucketAction,
		policy.GetBucketLocationAction,
		policy.GetBucketNotificationAction,
		policy.GetBucketPolicyAction,
		policy.GetBucketPolicyStatusAction,
		policy.GetBucketCorsAction,
		policy.GetBucketLifecycleAction,
		policy.GetBucketEncryptionAction,
		policy.GetBucketVersioningAction,
		policy.GetBucketTaggingAction,
		policy.GetBucketObjectLockConfigurationAction,
		policy.GetReplicationConfigurationAction,
		policy.GetObjectRetentionAction,
		policy.GetObjectLegalHoldAction,
		policy.GetObjectTaggingAction,
		policy.GetObjectVersionTaggingAction,
		policy.ListenNotificationAction,
		policy.ListenBucketNotificationAction,
		policy.ReplicateObjectAction:
		return admission.ClassRead

	case policy.PutObjectAction,
		policy.PutObjectFanOutAction,
		policy.DeleteObjectAction,
		policy.DeleteObjectVersionAction,
		policy.AbortMultipartUploadAction,
		policy.CreateBucketAction,
		policy.DeleteBucketAction,
		policy.ForceDeleteBucketAction,
		policy.DeleteBucketPolicyAction,
		policy.DeleteBucketCorsAction,
		policy.PutBucketLifecycleAction,
		policy.PutBucketNotificationAction,
		policy.PutBucketPolicyAction,
		policy.PutBucketCorsAction,
		policy.PutBucketEncryptionAction,
		policy.PutBucketVersioningAction,
		policy.PutBucketTaggingAction,
		policy.PutBucketObjectLockConfigurationAction,
		policy.PutReplicationConfigurationAction,
		policy.PutObjectRetentionAction,
		policy.PutObjectLegalHoldAction,
		policy.PutObjectTaggingAction,
		policy.PutObjectVersionTaggingAction,
		policy.DeleteObjectTaggingAction,
		policy.DeleteObjectVersionTaggingAction,
		policy.BypassGovernanceRetentionAction,
		policy.RestoreObjectAction,
		policy.ReplicateDeleteAction,
		policy.ReplicateTagsAction,
		policy.ResetBucketReplicationStateAction:
		return admission.ClassWrite
	}
	return ""
}

// requestActionClass classifies an S3 request: explicit action mapping first,
// HTTP method fallback for unmapped actions (GET/HEAD -> read, everything
// else -> write).
func requestActionClass(r *http.Request, a policy.Action) admission.ActionClass {
	if c := classifyAction(a); c != "" {
		return c
	}
	if r != nil {
		switch r.Method {
		case http.MethodGet, http.MethodHead:
			return admission.ClassRead
		}
	}
	return admission.ClassWrite
}

// admissionCaller resolves the caller dimension for an authenticated
// principal. STS temporary credentials are merged into their parent user so
// that rotating temporary credentials does not grant a fresh allowance;
// long-lived users and service accounts keep their own access key.
// Anonymous requests (empty access key) return "" and have no caller
// dimension.
func admissionCaller(cred auth.Credentials) string {
	if cred.IsTemp() {
		return cred.ParentUser
	}
	return cred.AccessKey
}
