// Copyright (c) 2024 MinIO, Inc.
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
	"bytes"
	"context"
	"io"
	"net/http"
	"sort"
	"strings"
	"sync"
	"time"
)

// fakeTransitionObject is one stored object in fakeTransitionStore.
type fakeTransitionObject struct {
	data    []byte
	modTime time.Time
}

// fakeTransitionStore is an in-memory transitionFailureStore for tests.
type fakeTransitionStore struct {
	mu      sync.Mutex
	objects map[string]fakeTransitionObject

	locksMu sync.Mutex
	locks   map[string]*sync.Mutex
}

func newFakeTransitionStore() *fakeTransitionStore {
	return &fakeTransitionStore{
		objects: make(map[string]fakeTransitionObject),
		locks:   make(map[string]*sync.Mutex),
	}
}

func (f *fakeTransitionStore) ListObjects(ctx context.Context, bucket, prefix, marker, delimiter string, maxKeys int) (ListObjectsInfo, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	var keys []string
	for k := range f.objects {
		if !strings.HasPrefix(k, prefix) {
			continue
		}
		if marker != "" && k <= marker {
			continue
		}
		keys = append(keys, k)
	}
	sort.Strings(keys)
	res := ListObjectsInfo{}
	for i, k := range keys {
		if i >= maxKeys {
			res.IsTruncated = true
			break
		}
		res.Objects = append(res.Objects, ObjectInfo{
			Bucket:  bucket,
			Name:    k,
			ModTime: f.objects[k].modTime,
			Size:    int64(len(f.objects[k].data)),
		})
	}
	if res.IsTruncated && len(res.Objects) > 0 {
		res.NextMarker = res.Objects[len(res.Objects)-1].Name
	}
	return res, nil
}

func (f *fakeTransitionStore) GetObjectNInfo(ctx context.Context, bucket, object string, rs *HTTPRangeSpec, h http.Header, opts ObjectOptions) (*GetObjectReader, error) {
	f.mu.Lock()
	obj, ok := f.objects[object]
	f.mu.Unlock()
	if !ok {
		return nil, errFileNotFound
	}
	return NewGetObjectReaderFromReader(bytes.NewReader(obj.data), ObjectInfo{Bucket: bucket, Name: object, Size: int64(len(obj.data))}, opts)
}

func (f *fakeTransitionStore) PutObject(ctx context.Context, bucket, object string, r *PutObjReader, opts ObjectOptions) (ObjectInfo, error) {
	data, err := io.ReadAll(r)
	if err != nil {
		return ObjectInfo{}, err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	f.objects[object] = fakeTransitionObject{data: data, modTime: time.Now()}
	return ObjectInfo{Bucket: bucket, Name: object, Size: int64(len(data))}, nil
}

func (f *fakeTransitionStore) DeleteObject(ctx context.Context, bucket, object string, opts ObjectOptions) (ObjectInfo, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	delete(f.objects, object)
	return ObjectInfo{Bucket: bucket, Name: object}, nil
}

// fakeTransitionLocker is a no-op RWLocker backed by a real mutex.
type fakeTransitionLocker struct {
	mu *sync.Mutex
}

func (l fakeTransitionLocker) GetLock(ctx context.Context, timeout *dynamicTimeout) (LockContext, error) {
	l.mu.Lock()
	cctx, cancel := context.WithCancel(ctx)
	return LockContext{ctx: cctx, cancel: cancel}, nil
}

func (l fakeTransitionLocker) Unlock(lkCtx LockContext) {
	lkCtx.Cancel()
	l.mu.Unlock()
}

func (l fakeTransitionLocker) GetRLock(ctx context.Context, timeout *dynamicTimeout) (LockContext, error) {
	return l.GetLock(ctx, timeout)
}

func (l fakeTransitionLocker) RUnlock(lkCtx LockContext) {
	l.Unlock(lkCtx)
}

func (f *fakeTransitionStore) NewNSLock(bucket string, objects ...string) RWLocker {
	key := bucket + "/" + strings.Join(objects, "/")
	f.locksMu.Lock()
	m, ok := f.locks[key]
	if !ok {
		m = &sync.Mutex{}
		f.locks[key] = m
	}
	f.locksMu.Unlock()
	return fakeTransitionLocker{mu: m}
}

// fakeTransitionObjectLayer makes fakeTransitionStore satisfy the big
// ObjectLayer interface; only the journal/health methods are ever called in
// transition failure tests, all other interface methods panic on use.
type fakeTransitionObjectLayer struct {
	ObjectLayer
	s *fakeTransitionStore
}

func (f *fakeTransitionObjectLayer) ListObjects(ctx context.Context, bucket, prefix, marker, delimiter string, maxKeys int) (ListObjectsInfo, error) {
	return f.s.ListObjects(ctx, bucket, prefix, marker, delimiter, maxKeys)
}

func (f *fakeTransitionObjectLayer) GetObjectNInfo(ctx context.Context, bucket, object string, rs *HTTPRangeSpec, h http.Header, opts ObjectOptions) (*GetObjectReader, error) {
	return f.s.GetObjectNInfo(ctx, bucket, object, rs, h, opts)
}

func (f *fakeTransitionObjectLayer) PutObject(ctx context.Context, bucket, object string, r *PutObjReader, opts ObjectOptions) (ObjectInfo, error) {
	return f.s.PutObject(ctx, bucket, object, r, opts)
}

func (f *fakeTransitionObjectLayer) DeleteObject(ctx context.Context, bucket, object string, opts ObjectOptions) (ObjectInfo, error) {
	return f.s.DeleteObject(ctx, bucket, object, opts)
}

func (f *fakeTransitionObjectLayer) NewNSLock(bucket string, objects ...string) RWLocker {
	return f.s.NewNSLock(bucket, objects...)
}

func (f *fakeTransitionStore) asObjectLayer() ObjectLayer {
	return &fakeTransitionObjectLayer{s: f}
}

// hasKey reports whether the store contains an object at p.
func (f *fakeTransitionStore) hasKey(p string) bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	_, ok := f.objects[p]
	return ok
}

// keyCount returns the total number of stored objects.
func (f *fakeTransitionStore) keyCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.objects)
}

// fakeTransitionDrive is an in-memory StorageAPI implementing only the
// methods used by the transition pending snapshot.
type fakeTransitionDrive struct {
	StorageAPI
	mu    sync.Mutex
	files map[string][]byte
}

func newFakeTransitionDrive() *fakeTransitionDrive {
	return &fakeTransitionDrive{files: make(map[string][]byte)}
}

func (d *fakeTransitionDrive) CreateFile(ctx context.Context, origvolume, volume, path string, size int64, reader io.Reader) error {
	data, err := io.ReadAll(reader)
	if err != nil {
		return err
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	d.files[volume+"/"+path] = data
	return nil
}

func (d *fakeTransitionDrive) ReadFileStream(ctx context.Context, volume, path string, offset, length int64) (io.ReadCloser, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	data, ok := d.files[volume+"/"+path]
	if !ok {
		return nil, errFileNotFound
	}
	return io.NopCloser(bytes.NewReader(data)), nil
}

func (d *fakeTransitionDrive) Delete(ctx context.Context, volume, path string, opts DeleteOptions) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	delete(d.files, volume+"/"+path)
	return nil
}

func (d *fakeTransitionDrive) fileCount() int {
	d.mu.Lock()
	defer d.mu.Unlock()
	return len(d.files)
}

func (d *fakeTransitionDrive) hasFile(name string) bool {
	d.mu.Lock()
	defer d.mu.Unlock()
	_, ok := d.files[name]
	return ok
}
