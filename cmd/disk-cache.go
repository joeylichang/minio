/*
 * MinIO Cloud Storage, (C) 2019,2020 MinIO, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package cmd

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/minio/minio/cmd/config/cache"
	"github.com/minio/minio/cmd/logger"
	objectlock "github.com/minio/minio/pkg/bucket/object/lock"
	"github.com/minio/minio/pkg/color"
	"github.com/minio/minio/pkg/sync/errgroup"
	"github.com/minio/minio/pkg/wildcard"
)

const (
	cacheBlkSize    = int64(1 * 1024 * 1024)
	cacheGCInterval = time.Minute * 30
)

// CacheStorageInfo - represents total, free capacity of
// underlying cache storage.
type CacheStorageInfo struct {
	Total uint64 // Total cache disk space.
	Free  uint64 // Free cache available space.
}

// CacheObjectLayer implements primitives for cache object API layer.
type CacheObjectLayer interface {
	// Object operations.
	GetObjectNInfo(ctx context.Context, bucket, object string, rs *HTTPRangeSpec, h http.Header, lockType LockType, opts ObjectOptions) (gr *GetObjectReader, err error)
	GetObjectInfo(ctx context.Context, bucket, object string, opts ObjectOptions) (objInfo ObjectInfo, err error)
	DeleteObject(ctx context.Context, bucket, object string, opts ObjectOptions) (ObjectInfo, error)
	DeleteObjects(ctx context.Context, bucket string, objects []ObjectToDelete, opts ObjectOptions) ([]DeletedObject, []error)
	PutObject(ctx context.Context, bucket, object string, data *PutObjReader, opts ObjectOptions) (objInfo ObjectInfo, err error)
	CopyObject(ctx context.Context, srcBucket, srcObject, destBucket, destObject string, srcInfo ObjectInfo, srcOpts, dstOpts ObjectOptions) (objInfo ObjectInfo, err error)
	// Storage operations.
	StorageInfo(ctx context.Context) CacheStorageInfo
	CacheStats() *CacheStats
}

// Abstracts disk caching - used by the S3 layer
type cacheObjects struct {
	// slice of cache drives
	cache []*diskCache
	// file path patterns to exclude from cache
	exclude []string
	// number of accesses after which to cache an object
	after int
	// if true migration is in progress from v1 to v2
	migrating bool
	// mutex to protect migration bool
	migMutex sync.Mutex

	// Cache stats
	cacheStats *CacheStats

	GetObjectNInfoFn func(ctx context.Context, bucket, object string, rs *HTTPRangeSpec, h http.Header, lockType LockType, opts ObjectOptions) (gr *GetObjectReader, err error)
	GetObjectInfoFn  func(ctx context.Context, bucket, object string, opts ObjectOptions) (objInfo ObjectInfo, err error)
	DeleteObjectFn   func(ctx context.Context, bucket, object string, opts ObjectOptions) (objInfo ObjectInfo, err error)
	PutObjectFn      func(ctx context.Context, bucket, object string, data *PutObjReader, opts ObjectOptions) (objInfo ObjectInfo, err error)
	CopyObjectFn     func(ctx context.Context, srcBucket, srcObject, destBucket, destObject string, srcInfo ObjectInfo, srcOpts, dstOpts ObjectOptions) (objInfo ObjectInfo, err error)
}

func (c *cacheObjects) incHitsToMeta(ctx context.Context, dcache *diskCache, bucket, object string, size int64, eTag string, rs *HTTPRangeSpec) error {
	metadata := make(map[string]string)
	metadata["etag"] = eTag
	return dcache.SaveMetadata(ctx, bucket, object, metadata, size, rs, "", true)
}

// Backend metadata could have changed through server side copy - reset cache metadata if that is the case
func (c *cacheObjects) updateMetadataIfChanged(ctx context.Context, dcache *diskCache, bucket, object string, bkObjectInfo, cacheObjInfo ObjectInfo, rs *HTTPRangeSpec) error {

	bkMeta := make(map[string]string)
	cacheMeta := make(map[string]string)
	for k, v := range bkObjectInfo.UserDefined {
		if strings.HasPrefix(strings.ToLower(k), ReservedMetadataPrefixLower) {
			// Do not need to send any internal metadata
			continue
		}
		bkMeta[http.CanonicalHeaderKey(k)] = v
	}
	for k, v := range cacheObjInfo.UserDefined {
		if strings.HasPrefix(strings.ToLower(k), ReservedMetadataPrefixLower) {
			// Do not need to send any internal metadata
			continue
		}
		cacheMeta[http.CanonicalHeaderKey(k)] = v
	}

	if !isMetadataSame(bkMeta, cacheMeta) ||
		bkObjectInfo.ETag != cacheObjInfo.ETag ||
		bkObjectInfo.ContentType != cacheObjInfo.ContentType ||
		!bkObjectInfo.Expires.Equal(cacheObjInfo.Expires) {
		return dcache.SaveMetadata(ctx, bucket, object, getMetadata(bkObjectInfo), bkObjectInfo.Size, nil, "", false)
	}
	return c.incHitsToMeta(ctx, dcache, bucket, object, cacheObjInfo.Size, cacheObjInfo.ETag, rs)
}

// DeleteObject clears cache entry if backend delete operation succeeds
/*
 * 1. 先删除磁盘再删除 cache
 */
func (c *cacheObjects) DeleteObject(ctx context.Context, bucket, object string, opts ObjectOptions) (objInfo ObjectInfo, err error) {
	if objInfo, err = c.DeleteObjectFn(ctx, bucket, object, opts); err != nil {
		return
	}
	if c.isCacheExclude(bucket, object) || c.skipCache() {
		return
	}

	dcache, cerr := c.getCacheLoc(bucket, object)
	if cerr != nil {
		return objInfo, cerr
	}
	dcache.Delete(ctx, bucket, object)
	return
}

// DeleteObjects batch deletes objects in slice, and clears any cached entries
func (c *cacheObjects) DeleteObjects(ctx context.Context, bucket string, objects []ObjectToDelete, opts ObjectOptions) ([]DeletedObject, []error) {
	errs := make([]error, len(objects))
	objInfos := make([]ObjectInfo, len(objects))
	for idx, object := range objects {
		opts.VersionID = object.VersionID
		objInfos[idx], errs[idx] = c.DeleteObject(ctx, bucket, object.ObjectName, opts)
	}
	deletedObjects := make([]DeletedObject, len(objInfos))
	for idx := range errs {
		if errs[idx] != nil {
			continue
		}
		if objInfos[idx].DeleteMarker {
			deletedObjects[idx] = DeletedObject{
				DeleteMarker:          objInfos[idx].DeleteMarker,
				DeleteMarkerVersionID: objInfos[idx].VersionID,
			}
			continue
		}
		deletedObjects[idx] = DeletedObject{
			ObjectName: objInfos[idx].Name,
			VersionID:  objInfos[idx].VersionID,
		}
	}
	return deletedObjects, errs
}

// construct a metadata k-v map
func getMetadata(objInfo ObjectInfo) map[string]string {
	metadata := make(map[string]string)
	metadata["etag"] = objInfo.ETag
	metadata["content-type"] = objInfo.ContentType
	if objInfo.ContentEncoding != "" {
		metadata["content-encoding"] = objInfo.ContentEncoding
	}
	if objInfo.Expires != timeSentinel {
		metadata["expires"] = objInfo.Expires.Format(http.TimeFormat)
	}
	for k, v := range objInfo.UserDefined {
		metadata[k] = v
	}
	return metadata
}

// marks cache hit
func (c *cacheObjects) incCacheStats(size int64) {
	c.cacheStats.incHit()
	c.cacheStats.incBytesServed(size)
}

/*
 * for mutilpy put 
 * 如果不在 cache 会put 到 cache 中
 */
func (c *cacheObjects) GetObjectNInfo(ctx context.Context, bucket, object string, rs *HTTPRangeSpec, h http.Header, lockType LockType, opts ObjectOptions) (gr *GetObjectReader, err error) {
	if c.isCacheExclude(bucket, object) || c.skipCache() {
		return c.GetObjectNInfoFn(ctx, bucket, object, rs, h, lockType, opts)
	}
	var cc *cacheControl
	var cacheObjSize int64
	// fetch diskCache if object is currently cached or nearest available cache drive
	dcache, err := c.getCacheToLoc(ctx, bucket, object)
	if err != nil {
		return c.GetObjectNInfoFn(ctx, bucket, object, rs, h, lockType, opts)
	}

	cacheReader, numCacheHits, cacheErr := dcache.Get(ctx, bucket, object, rs, h, opts)
	if cacheErr == nil {
		cacheObjSize = cacheReader.ObjInfo.Size
		if rs != nil {
			if _, len, err := rs.GetOffsetLength(cacheObjSize); err == nil {
				cacheObjSize = len
			}
		}
		cc = cacheControlOpts(cacheReader.ObjInfo)
		if cc != nil && (!cc.isStale(cacheReader.ObjInfo.ModTime) ||
			cc.onlyIfCached) {
			// This is a cache hit, mark it so
			bytesServed := cacheReader.ObjInfo.Size
			if rs != nil {
				if _, len, err := rs.GetOffsetLength(bytesServed); err == nil {
					bytesServed = len
				}
			}
			c.cacheStats.incHit()
			c.cacheStats.incBytesServed(bytesServed)
			c.incHitsToMeta(ctx, dcache, bucket, object, cacheReader.ObjInfo.Size, cacheReader.ObjInfo.ETag, rs)
			return cacheReader, nil
		}
		if cc != nil && cc.noStore {
			cacheReader.Close()
			c.cacheStats.incMiss()
			bReader, err := c.GetObjectNInfo(ctx, bucket, object, rs, h, lockType, opts)
			bReader.ObjInfo.CacheLookupStatus = CacheHit
			bReader.ObjInfo.CacheStatus = CacheMiss
			return bReader, err
		}
	}

	objInfo, err := c.GetObjectInfoFn(ctx, bucket, object, opts)
	if backendDownError(err) && cacheErr == nil {
		c.incCacheStats(cacheObjSize)
		return cacheReader, nil
	} else if err != nil {
		if cacheErr == nil {
			cacheReader.Close()
		}
		if _, ok := err.(ObjectNotFound); ok {
			if cacheErr == nil {
				// Delete cached entry if backend object
				// was deleted.
				dcache.Delete(ctx, bucket, object)
			}
		}
		c.cacheStats.incMiss()
		return nil, err
	}

	if !objInfo.IsCacheable() {
		if cacheErr == nil {
			cacheReader.Close()
		}
		c.cacheStats.incMiss()
		return c.GetObjectNInfoFn(ctx, bucket, object, rs, h, lockType, opts)
	}
	// skip cache for objects with locks
	objRetention := objectlock.GetObjectRetentionMeta(objInfo.UserDefined)
	legalHold := objectlock.GetObjectLegalHoldMeta(objInfo.UserDefined)
	if objRetention.Mode.Valid() || legalHold.Status.Valid() {
		if cacheErr == nil {
			cacheReader.Close()
		}
		c.cacheStats.incMiss()
		return c.GetObjectNInfoFn(ctx, bucket, object, rs, h, lockType, opts)
	}
	if cacheErr == nil {
		// if ETag matches for stale cache entry, serve from cache
		if cacheReader.ObjInfo.ETag == objInfo.ETag {
			// Update metadata in case server-side copy might have changed object metadata
			c.updateMetadataIfChanged(ctx, dcache, bucket, object, objInfo, cacheReader.ObjInfo, rs)
			c.incCacheStats(cacheObjSize)
			return cacheReader, nil
		}
		cacheReader.Close()
		// Object is stale, so delete from cache
		dcache.Delete(ctx, bucket, object)
	}

	// Reaching here implies cache miss
	c.cacheStats.incMiss()

	// Since we got here, we are serving the request from backend,
	// and also adding the object to the cache.
	if dcache.diskUsageHigh() {
		dcache.triggerGC <- struct{}{} // this is non-blocking
	}

	bkReader, bkErr := c.GetObjectNInfoFn(ctx, bucket, object, rs, h, lockType, opts)

	if bkErr != nil {
		return bkReader, bkErr
	}
	// If object has less hits than configured cache after, just increment the hit counter
	// but do not cache it.
	if numCacheHits < c.after {
		c.incHitsToMeta(ctx, dcache, bucket, object, objInfo.Size, objInfo.ETag, rs)
		return bkReader, bkErr
	}

	// Record if cache has a hit that was invalidated by ETag verification
	if cacheErr == nil {
		bkReader.ObjInfo.CacheLookupStatus = CacheHit
	}
	if !dcache.diskAvailable(objInfo.Size) {
		return bkReader, bkErr
	}

	if rs != nil {
		go func() {
			// if range caching is disabled, download entire object.
			if !dcache.enableRange {
				rs = nil
			}
			// fill cache in the background for range GET requests
			bReader, bErr := c.GetObjectNInfoFn(ctx, bucket, object, rs, h, lockType, opts)
			if bErr != nil {
				return
			}
			defer bReader.Close()
			oi, _, _, err := dcache.statRange(ctx, bucket, object, rs)
			// avoid cache overwrite if another background routine filled cache
			if err != nil || oi.ETag != bReader.ObjInfo.ETag {
				// use a new context to avoid locker prematurely timing out operation when the GetObjectNInfo returns.
				dcache.Put(context.Background(), bucket, object, bReader, bReader.ObjInfo.Size, rs, ObjectOptions{UserDefined: getMetadata(bReader.ObjInfo)}, false)
				return
			}
		}()
		return bkReader, bkErr
	}

	// Initialize pipe.
	pipeReader, pipeWriter := io.Pipe()
	teeReader := io.TeeReader(bkReader, pipeWriter)
	go func() {
		putErr := dcache.Put(ctx, bucket, object, io.LimitReader(pipeReader, bkReader.ObjInfo.Size), bkReader.ObjInfo.Size, nil, ObjectOptions{UserDefined: getMetadata(bkReader.ObjInfo)}, false)
		// close the write end of the pipe, so the error gets
		// propagated to getObjReader
		pipeWriter.CloseWithError(putErr)
	}()
	cleanupBackend := func() { bkReader.Close() }
	cleanupPipe := func() { pipeWriter.Close() }
	return NewGetObjectReaderFromReader(teeReader, bkReader.ObjInfo, opts, cleanupBackend, cleanupPipe)
}

// Returns ObjectInfo from cache if available.
/*
 * 1. 是否在 cache 的 exclude 里面
 * 2. 如 put 获取 cache 磁盘位置，通过 state 接口获取 cachedObjInfo
 * 3. 根据 cachedObjInfo 判断是否过期，没有则返回 cachedObjInfo
 * 4. 到此，cache 没有命中，调用 zone 的接口
 * 注意：cache 的统计信息的更新
 */
func (c *cacheObjects) GetObjectInfo(ctx context.Context, bucket, object string, opts ObjectOptions) (ObjectInfo, error) {
	getObjectInfoFn := c.GetObjectInfoFn

	if c.isCacheExclude(bucket, object) || c.skipCache() {
		return getObjectInfoFn(ctx, bucket, object, opts)
	}

	// fetch diskCache if object is currently cached or nearest available cache drive
	dcache, err := c.getCacheToLoc(ctx, bucket, object)
	if err != nil {
		return getObjectInfoFn(ctx, bucket, object, opts)
	}
	var cc *cacheControl
	// if cache control setting is valid, avoid HEAD operation to backend
	cachedObjInfo, _, cerr := dcache.Stat(ctx, bucket, object)
	if cerr == nil {
		cc = cacheControlOpts(cachedObjInfo)
		if cc == nil || (cc != nil && !cc.isStale(cachedObjInfo.ModTime)) {
			// This is a cache hit, mark it so
			c.cacheStats.incHit()
			return cachedObjInfo, nil
		}
	}

	objInfo, err := getObjectInfoFn(ctx, bucket, object, opts)
	if err != nil {
		if _, ok := err.(ObjectNotFound); ok {
			// Delete the cached entry if backend object was deleted.
			dcache.Delete(ctx, bucket, object)
			c.cacheStats.incMiss()
			return ObjectInfo{}, err
		}
		if !backendDownError(err) {
			c.cacheStats.incMiss()
			return ObjectInfo{}, err
		}
		if cerr == nil {
			// This is a cache hit, mark it so
			c.cacheStats.incHit()
			return cachedObjInfo, nil
		}
		c.cacheStats.incMiss()
		return ObjectInfo{}, BackendDown{}
	}
	// Reaching here implies cache miss
	c.cacheStats.incMiss()
	// when backend is up, do a sanity check on cached object
	if cerr != nil {
		return objInfo, nil
	}
	if cachedObjInfo.ETag != objInfo.ETag {
		// Delete the cached entry if the backend object was replaced.
		dcache.Delete(ctx, bucket, object)
	}
	return objInfo, nil
}

// CopyObject reverts to backend after evicting any stale cache entries
/*
 * 1. 是否在 cache 的 exclude 目录、不是原地copy（原地copy会更新元数据），直接调用 zone 的 copy 接口
 * 2. 如果是原地 copy，根据 user_define 的 cache 策略决定是否过期，没有cache策略或者过期则删除cache
 * 3. 进行原地 copy
 */
func (c *cacheObjects) CopyObject(ctx context.Context, srcBucket, srcObject, dstBucket, dstObject string, srcInfo ObjectInfo, srcOpts, dstOpts ObjectOptions) (objInfo ObjectInfo, err error) {
	copyObjectFn := c.CopyObjectFn
	if c.isCacheExclude(srcBucket, srcObject) || c.skipCache() {
		return copyObjectFn(ctx, srcBucket, srcObject, dstBucket, dstObject, srcInfo, srcOpts, dstOpts)
	}
	if srcBucket != dstBucket || srcObject != dstObject {
		return copyObjectFn(ctx, srcBucket, srcObject, dstBucket, dstObject, srcInfo, srcOpts, dstOpts)
	}
	// fetch diskCache if object is currently cached or nearest available cache drive
	dcache, err := c.getCacheToLoc(ctx, srcBucket, srcObject)
	if err != nil {
		return copyObjectFn(ctx, srcBucket, srcObject, dstBucket, dstObject, srcInfo, srcOpts, dstOpts)
	}
	// if currently cached, evict old entry and revert to backend.
	if cachedObjInfo, _, cerr := dcache.Stat(ctx, srcBucket, srcObject); cerr == nil {
		cc := cacheControlOpts(cachedObjInfo)
		if cc == nil || !cc.isStale(cachedObjInfo.ModTime) {
			dcache.Delete(ctx, srcBucket, srcObject)
		}
	}
	return copyObjectFn(ctx, srcBucket, srcObject, dstBucket, dstObject, srcInfo, srcOpts, dstOpts)
}

// StorageInfo - returns underlying storage statistics.
func (c *cacheObjects) StorageInfo(ctx context.Context) (cInfo CacheStorageInfo) {
	var total, free uint64
	for _, cache := range c.cache {
		if cache == nil {
			continue
		}
		info, err := getDiskInfo(cache.dir)
		logger.GetReqInfo(ctx).AppendTags("cachePath", cache.dir)
		logger.LogIf(ctx, err)
		total += info.Total
		free += info.Free
	}
	return CacheStorageInfo{
		Total: total,
		Free:  free,
	}
}

// CacheStats - returns underlying storage statistics.
func (c *cacheObjects) CacheStats() (cs *CacheStats) {
	return c.cacheStats
}

// skipCache() returns true if cache migration is in progress
func (c *cacheObjects) skipCache() bool {
	c.migMutex.Lock()
	defer c.migMutex.Unlock()
	return c.migrating
}

// Returns true if object should be excluded from cache
func (c *cacheObjects) isCacheExclude(bucket, object string) bool {
	// exclude directories from cache
	if strings.HasSuffix(object, SlashSeparator) {
		return true
	}
	for _, pattern := range c.exclude {
		matchStr := fmt.Sprintf("%s/%s", bucket, object)
		if ok := wildcard.MatchSimple(pattern, matchStr); ok {
			return true
		}
	}
	return false
}

// choose a cache deterministically based on hash of bucket,object. The hash index is treated as
// a hint. In the event that the cache drive at hash index is offline, treat the list of cache drives
// as a circular buffer and walk through them starting at hash index until an online drive is found.
func (c *cacheObjects) getCacheLoc(bucket, object string) (*diskCache, error) {
	index := c.hashIndex(bucket, object)
	numDisks := len(c.cache)
	for k := 0; k < numDisks; k++ {
		i := (index + k) % numDisks
		if c.cache[i] == nil {
			continue
		}
		if c.cache[i].IsOnline() {
			return c.cache[i], nil
		}
	}
	return nil, errDiskNotFound
}

// get cache disk where object is currently cached for a GET operation. If object does not exist at that location,
// treat the list of cache drives as a circular buffer and walk through them starting at hash index
// until an online drive is found.If object is not found, fall back to the first online cache drive
// closest to the hash index, so that object can be re-cached.
func (c *cacheObjects) getCacheToLoc(ctx context.Context, bucket, object string) (*diskCache, error) {
	index := c.hashIndex(bucket, object)

	numDisks := len(c.cache)
	// save first online cache disk closest to the hint index
	var firstOnlineDisk *diskCache
	for k := 0; k < numDisks; k++ {
		i := (index + k) % numDisks
		if c.cache[i] == nil {
			continue
		}
		if c.cache[i].IsOnline() {
			if firstOnlineDisk == nil {
				firstOnlineDisk = c.cache[i]
			}
			if c.cache[i].Exists(ctx, bucket, object) {
				return c.cache[i], nil
			}
		}
	}

	if firstOnlineDisk != nil {
		return firstOnlineDisk, nil
	}
	return nil, errDiskNotFound
}

// Compute a unique hash sum for bucket and object
func (c *cacheObjects) hashIndex(bucket, object string) int {
	return crcHashMod(pathJoin(bucket, object), len(c.cache))
}

// newCache initializes the cacheFSObjects for the "drives" specified in config.json
// or the global env overrides.
/*
 * 参数：
 * 	type Config struct {
 *		Enabled       bool    
 *		Drives        []string  // 需要cache的磁盘
 *		Expiry        int      
 *		MaxUse        int      
 *		Quota         int      
 *		Exclude       []string 
 *		After         int      
 *		WatermarkLow  int      
 *		WatermarkHigh int      
 *		Range         bool     
 *	}
 *
 * 返回值
 *	 type diskCache struct {
 *		online       uint32 	// 1 代表在线，0 代表下线
 *		purgeRunning int32 		// 是否在 gc
 *
 *		triggerGC     chan struct{} 	// 触发 gc 的通信管道
 *		dir           string         	// caching directory
 *
 *		stats         CacheDiskStats 	// disk cache stats for prometheus
 *	 		type CacheDiskStats struct {
 *				UsageState int32 		// high value is '1', low its '0'
 *				UsagePercent uint64
 *				Dir          string
 *			}
 * 			// high >= quotaPct * highWatermark ? true : false
 *			// low  <= quotaPct * lowWatermark ? true : false
 *
 *		quotaPct      int           // 缓存容量 = quotaPct * 磁盘容量
 *		pool          sync.Pool 	// 4K 的内存空间对象，与直接IO大小对应
 *		after         int 			// 应该是被访问了 after 次之后，才进行cache
 *		lowWatermark  int 			// gc 的停止点 quotaPct * lowWatermark
 *		highWatermark int 			// 最大空间 quotaPct * highWatermark
 *		enableRange   bool 			// 是否支持 分片cache
 *		nsMutex *nsLockMap 			// 分布式锁，下面是回调
 *		NewNSLockFn func(ctx context.Context, cachePath string) RWLocker
 *	}
 */
func newCache(config cache.Config) ([]*diskCache, bool, error) {
	var caches []*diskCache
	ctx := logger.SetReqInfo(GlobalContext, &logger.ReqInfo{})
	/*
	 * 注意： 
	 * 	1. formatCacheV2 与 formatErasureV3 是不同的结构体，内部都继承 formatMetaV1
	 * 	2. 后者是 newObjectLayer 时初始化并保存的 
	 * 
	 * type formatCacheV2 = formatCacheV1
	 * type formatCacheV1 struct {
	 *		formatMetaV1
	 *		type formatMetaV1 struct {
	 *			Version string 	// Version of the format config
	 *			Format string 	// backend format type, we force to xl
	 *			ID string 		// deployment id 
	 *		}
	 *		Cache struct {
	 *			Version string 	// Version of 'cache' format.
	 *			This    string 	// disk uuid.
	 *			Disks []string 	// input disk order generated the first time when fresh disks were supplied.
	 *			DistributionAlgo string 	// hashing algorithm
	 *		} 		// Cache field holds cache format.
	 *	}
	 *
	 * loadAndValidateCacheFormat 从 /diskpath/.minio.sys/format.json 里面读取
	 * 一块磁盘对应一个 format
	 * migrating 标识 format.json 是否进行升级搞得版本
	 *
	 * loadAndValidateCacheFormat 内部只对本节点进行了加载 format.json
	 * 应该是只 cache 了本地磁盘的数据，为什么用 cache 呢？直接读数据效果不也是一样的吗？都是直接IO
	 * 难道是考虑，磁盘介质提升 cache 性能？
	 */
	formats, migrating, err := loadAndValidateCacheFormat(ctx, config.Drives)
	if err != nil {
		return nil, false, err
	}
	for i, dir := range config.Drives {
		// skip diskCache creation for cache drives missing a format.json
		/*
		 * 只对 foemat.json 的磁盘，既正常的磁盘进行 cache
		 */
		if formats[i] == nil {
			caches = append(caches, nil)
			continue
		}
		/*
		 * 判断系统是否支持访问次数查询
		 */
		if err := checkAtimeSupport(dir); err != nil {
			return nil, false, errors.New("Atime support required for disk caching")
		}

		cache, err := newDiskCache(ctx, dir, config)
		if err != nil {
			return nil, false, err
		}
		caches = append(caches, cache)
	}
	return caches, migrating, nil
}

func (c *cacheObjects) migrateCacheFromV1toV2(ctx context.Context) {
	logStartupMessage(color.Blue("Cache migration initiated ...."))

	g := errgroup.WithNErrs(len(c.cache))
	for index, dc := range c.cache {
		if dc == nil {
			continue
		}
		index := index
		g.Go(func() error {
			// start migration from V1 to V2
			return migrateOldCache(ctx, c.cache[index])
		}, index)
	}

	errCnt := 0
	for _, err := range g.Wait() {
		if err != nil {
			errCnt++
			logger.LogIf(ctx, err)
			continue
		}
	}

	if errCnt > 0 {
		return
	}

	// update migration status
	c.migMutex.Lock()
	defer c.migMutex.Unlock()
	c.migrating = false
	logStartupMessage(color.Blue("Cache migration completed successfully."))
}

// PutObject - caches the uploaded object for single Put operations
/*
 * 1. 获取cache disk
 * 	1.1 index = crchash(bucket/object) / len(diskcaches)
 * 	1.2 从 index 开始之后开始找，知道找到 object 在那个 disk ，直接返回
 * 	1.3 没有找到，则是从index 开始第一个 online 的disk
 * 2. formate 升级ing、cache 磁盘容量不够、需要 SSE、S3语义层带有锁、cache exclude目录
 * 		上述情况都不会写入cache，直接写入走 zone 的接口进行写
 * 3. 经过上述约束检查之后，同步写入数据磁盘，然后异步写入 cache
 *
 * 注意：
 * 	0. cache 的数据是 objectinfo，而不是用户的数据
 * 	1. cache 目录不与数据目录一一对应
 * 	2. cache 是 write through 方式
 * 	3. cache 与 data 磁盘应该是利用介质不同，提升读性能，不是内存cache
 */
func (c *cacheObjects) PutObject(ctx context.Context, bucket, object string, r *PutObjReader, opts ObjectOptions) (objInfo ObjectInfo, err error) {
	putObjectFn := c.PutObjectFn
	/*
	 * 1. index = crchash(bucket/object) / len(diskcaches)
	 * 2. 从 index 开始之后开始找，知道找到 object 在那个 disk ，直接返回
	 * 3. 没有找到，则是从index 开始第一个 online 的disk
	 */
	dcache, err := c.getCacheToLoc(ctx, bucket, object)
	if err != nil {
		// disk cache could not be located,execute backend call.
		/*
		 * 直接写数据目录
		 */
		return putObjectFn(ctx, bucket, object, r, opts)
	}
	size := r.Size()
	if c.skipCache() {
		return putObjectFn(ctx, bucket, object, r, opts)
	}

	// fetch from backend if there is no space on cache drive
	if !dcache.diskAvailable(size) {
		return putObjectFn(ctx, bucket, object, r, opts)
	}

	/*
	 * cache 不进行加密
	 */
	if opts.ServerSideEncryption != nil {
		dcache.Delete(ctx, bucket, object)
		return putObjectFn(ctx, bucket, object, r, opts)
	}

	// skip cache for objects with locks
	objRetention := objectlock.GetObjectRetentionMeta(opts.UserDefined)
	legalHold := objectlock.GetObjectLegalHoldMeta(opts.UserDefined)
	if objRetention.Mode.Valid() || legalHold.Status.Valid() {
		dcache.Delete(ctx, bucket, object)
		return putObjectFn(ctx, bucket, object, r, opts)
	}

	// fetch from backend if cache exclude pattern or cache-control
	// directive set to exclude
	if c.isCacheExclude(bucket, object) {
		dcache.Delete(ctx, bucket, object)
		return putObjectFn(ctx, bucket, object, r, opts)
	}

	/*
	 * write through 方式，先写入磁盘，在写入缓存，缓存完全是为了 get 
	 * 写入磁盘算成功，异步写入cache
	 */
	objInfo, err = putObjectFn(ctx, bucket, object, r, opts)

	if err == nil {
		go func() {
			// fill cache in the background
			bReader, bErr := c.GetObjectNInfoFn(ctx, bucket, object, nil, http.Header{}, readLock, ObjectOptions{})
			if bErr != nil {
				return
			}
			defer bReader.Close()
			oi, _, err := dcache.Stat(ctx, bucket, object)
			// avoid cache overwrite if another background routine filled cache
			if err != nil || oi.ETag != bReader.ObjInfo.ETag {
				dcache.Put(ctx, bucket, object, bReader, bReader.ObjInfo.Size, nil, ObjectOptions{UserDefined: getMetadata(bReader.ObjInfo)}, false)
			}
		}()
	}

	return objInfo, err

}

// Returns cacheObjects for use by Server.
/*
 * 参数：
 * 	type Config struct {
 *		Enabled       bool    
 *		Drives        []string 
 *		Expiry        int      
 *		MaxUse        int      
 *		Quota         int      
 *		Exclude       []string 
 *		After         int      
 *		WatermarkLow  int      
 *		WatermarkHigh int      
 *		Range         bool     
 *	}
 *
 * 返回值：
 * 	type cacheObjects struct {
 *		cache []*diskCache 	// cache 的磁盘，或者说路径
 *		exclude []string 	// 不被cache 的路径
 *		after int 			// 访问到一定次数之后才进行cache
 *		migrating bool 		// 元数据是否需要升级的标志位
 *		migMutex sync.Mutex
 *
 *		cacheStats *CacheStats
 *			type CacheStats struct {
 *				BytesServed  uint64 	// cache 的大小
 *				Hits         uint64 	// 命中次数
 *				Misses       uint64 	// 未命中技术
 *				GetDiskStats func() []CacheDiskStats 	// 针对磁盘的统计
 *					type CacheDiskStats struct {
 *						UsageState int32 		// high value is '1', low its '0'
 *						UsagePercent uint64
 *						Dir          string
 *					}
 *					// high >= quotaPct * highWatermark ? true : false
 *					// low  <= quotaPct * lowWatermark ? true : false
 *			}
 *	}
 */
func newServerCacheObjects(ctx context.Context, config cache.Config) (CacheObjectLayer, error) {
	// list of disk caches for cache "drives" specified in config.json or MINIO_CACHE_DRIVES env var.
	cache, migrateSw, err := newCache(config)
	if err != nil {
		return nil, err
	}
	c := &cacheObjects{
		cache:      cache,
		exclude:    config.Exclude,
		after:      config.After,
		migrating:  migrateSw,
		migMutex:   sync.Mutex{},
		cacheStats: newCacheStats(),
		GetObjectInfoFn: func(ctx context.Context, bucket, object string, opts ObjectOptions) (ObjectInfo, error) {
			return newObjectLayerFn().GetObjectInfo(ctx, bucket, object, opts)
		},
		GetObjectNInfoFn: func(ctx context.Context, bucket, object string, rs *HTTPRangeSpec, h http.Header, lockType LockType, opts ObjectOptions) (gr *GetObjectReader, err error) {
			return newObjectLayerFn().GetObjectNInfo(ctx, bucket, object, rs, h, lockType, opts)
		},
		DeleteObjectFn: func(ctx context.Context, bucket, object string, opts ObjectOptions) (ObjectInfo, error) {
			return newObjectLayerFn().DeleteObject(ctx, bucket, object, opts)
		},
		PutObjectFn: func(ctx context.Context, bucket, object string, data *PutObjReader, opts ObjectOptions) (objInfo ObjectInfo, err error) {
			return newObjectLayerFn().PutObject(ctx, bucket, object, data, opts)
		},
		CopyObjectFn: func(ctx context.Context, srcBucket, srcObject, destBucket, destObject string, srcInfo ObjectInfo, srcOpts, dstOpts ObjectOptions) (objInfo ObjectInfo, err error) {
			return newObjectLayerFn().CopyObject(ctx, srcBucket, srcObject, destBucket, destObject, srcInfo, srcOpts, dstOpts)
		},
	}
	c.cacheStats.GetDiskStats = func() []CacheDiskStats {
		cacheDiskStats := make([]CacheDiskStats, len(c.cache))
		for i := range c.cache {
			dcache := c.cache[i]
			cacheDiskStats[i] = CacheDiskStats{}
			if dcache != nil {
				cacheDiskStats[i].Dir = dcache.stats.Dir
				atomic.StoreInt32(&cacheDiskStats[i].UsageState, atomic.LoadInt32(&dcache.stats.UsageState))
				atomic.StoreUint64(&cacheDiskStats[i].UsagePercent, atomic.LoadUint64(&dcache.stats.UsagePercent))
			}
		}
		return cacheDiskStats
	}
	if migrateSw {
		go c.migrateCacheFromV1toV2(ctx)
	}
	go c.gc(ctx)
	return c, nil
}

/*
 * 1. 30min 遍历所有的磁盘进行gc，详细逻辑见 diskcache purg
 * 2. 另一种触发单盘 gc 的场景是，put 数据的时候超过 pct * high_water
 */
func (c *cacheObjects) gc(ctx context.Context) {
	/*
	 * 30 min 
	 */
	ticker := time.NewTicker(cacheGCInterval)

	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if c.migrating {
				continue
			}
			/*
			 * 逐个磁盘遍历完成 gc
			 */
			for _, dcache := range c.cache {
				if dcache != nil {
					dcache.triggerGC <- struct{}{}
				}
			}
		}
	}
}
