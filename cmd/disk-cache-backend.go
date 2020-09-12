/*
 * MinIO Cloud Storage, (C) 2019-2020 MinIO, Inc.
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
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/djherbis/atime"
	"github.com/minio/minio/cmd/config/cache"
	"github.com/minio/minio/cmd/crypto"
	xhttp "github.com/minio/minio/cmd/http"
	"github.com/minio/minio/cmd/logger"
	"github.com/minio/minio/pkg/disk"
	"github.com/minio/sio"
)

const (
	// cache.json object metadata for cached objects.
	cacheMetaJSONFile = "cache.json"
	cacheDataFile     = "part.1"
	cacheMetaVersion  = "1.0.0"
	cacheExpiryDays   = time.Duration(90 * time.Hour * 24) // defaults to 90 days
	// SSECacheEncrypted is the metadata key indicating that the object
	// is a cache entry encrypted with cache KMS master key in globalCacheKMS.
	SSECacheEncrypted = "X-Minio-Internal-Encrypted-Cache"
)

// CacheChecksumInfoV1 - carries checksums of individual blocks on disk.
type CacheChecksumInfoV1 struct {
	Algorithm string `json:"algorithm"`
	Blocksize int64  `json:"blocksize"`
}

// Represents the cache metadata struct
type cacheMeta struct {
	Version string   `json:"version"`
	Stat    StatInfo `json:"stat"` // Stat of the current object `cache.json`.

	// checksums of blocks on disk.
	Checksum CacheChecksumInfoV1 `json:"checksum,omitempty"`
	// Metadata map for current object.
	Meta map[string]string `json:"meta,omitempty"`
	// Ranges maps cached range to associated filename.
	Ranges map[string]string `json:"ranges,omitempty"`
	// Hits is a counter on the number of times this object has been accessed so far.
	Hits int `json:"hits,omitempty"`
}

// RangeInfo has the range, file and range length information for a cached range.
type RangeInfo struct {
	Range string
	File  string
	Size  int64
}

// Empty returns true if this is an empty struct
func (r *RangeInfo) Empty() bool {
	return r.Range == "" && r.File == "" && r.Size == 0
}

func (m *cacheMeta) ToObjectInfo(bucket, object string) (o ObjectInfo) {
	if len(m.Meta) == 0 {
		m.Meta = make(map[string]string)
		m.Stat.ModTime = timeSentinel
	}

	o = ObjectInfo{
		Bucket:            bucket,
		Name:              object,
		CacheStatus:       CacheHit,
		CacheLookupStatus: CacheHit,
	}

	// We set file info only if its valid.
	o.ModTime = m.Stat.ModTime
	o.Size = m.Stat.Size
	o.ETag = extractETag(m.Meta)
	o.ContentType = m.Meta["content-type"]
	o.ContentEncoding = m.Meta["content-encoding"]
	if storageClass, ok := m.Meta[xhttp.AmzStorageClass]; ok {
		o.StorageClass = storageClass
	} else {
		o.StorageClass = globalMinioDefaultStorageClass
	}
	var (
		t time.Time
		e error
	)
	if exp, ok := m.Meta["expires"]; ok {
		if t, e = time.Parse(http.TimeFormat, exp); e == nil {
			o.Expires = t.UTC()
		}
	}
	// etag/md5Sum has already been extracted. We need to
	// remove to avoid it from appearing as part of user-defined metadata
	o.UserDefined = cleanMetadata(m.Meta)
	return o
}

// represents disk cache struct
type diskCache struct {
	// is set to 0 if drive is offline
	online       uint32 // ref: https://golang.org/pkg/sync/atomic/#pkg-note-BUG
	purgeRunning int32

	triggerGC     chan struct{}
	dir           string         // caching directory
	stats         CacheDiskStats // disk cache stats for prometheus
	quotaPct      int            // max usage in %
	pool          sync.Pool
	after         int // minimum accesses before an object is cached.
	lowWatermark  int
	highWatermark int
	enableRange   bool
	// nsMutex namespace lock
	nsMutex *nsLockMap
	// Object functions pointing to the corresponding functions of backend implementation.
	NewNSLockFn func(ctx context.Context, cachePath string) RWLocker
}

// Inits the disk cache dir if it is not initialized already.
func newDiskCache(ctx context.Context, dir string, config cache.Config) (*diskCache, error) {
	quotaPct := config.MaxUse
	if quotaPct == 0 {
		quotaPct = config.Quota
	}

	if err := os.MkdirAll(dir, 0777); err != nil {
		return nil, fmt.Errorf("Unable to initialize '%s' dir, %w", dir, err)
	}
	cache := diskCache{
		dir:           dir,
		triggerGC:     make(chan struct{}),
		stats:         CacheDiskStats{Dir: dir},
		quotaPct:      quotaPct,
		after:         config.After,
		lowWatermark:  config.WatermarkLow,
		highWatermark: config.WatermarkHigh,
		enableRange:   config.Range,
		online:        1,
		pool: sync.Pool{
			New: func() interface{} {
				b := disk.AlignedBlock(int(cacheBlkSize))
				return &b
			},
		},
		nsMutex: newNSLock(false),
	}
	go cache.purgeWait(ctx)
	cache.diskUsageHigh() // update if cache usage is already high.
	cache.NewNSLockFn = func(ctx context.Context, cachePath string) RWLocker {
		return cache.nsMutex.NewNSLock(ctx, nil, cachePath, "")
	}
	return &cache, nil
}

// diskUsageLow() returns true if disk usage falls below the low watermark w.r.t configured cache quota.
// Ex. for a 100GB disk, if quota is configured as 70%  and watermark_low = 80% and
// watermark_high = 90% then garbage collection starts when 63% of disk is used and
// stops when disk usage drops to 56%
func (c *diskCache) diskUsageLow() bool {
	gcStopPct := c.quotaPct * c.lowWatermark / 100
	di, err := disk.GetInfo(c.dir)
	if err != nil {
		reqInfo := (&logger.ReqInfo{}).AppendTags("cachePath", c.dir)
		ctx := logger.SetReqInfo(GlobalContext, reqInfo)
		logger.LogIf(ctx, err)
		return false
	}
	usedPercent := (di.Total - di.Free) * 100 / di.Total
	low := int(usedPercent) < gcStopPct
	atomic.StoreUint64(&c.stats.UsagePercent, usedPercent)
	if low {
		atomic.StoreInt32(&c.stats.UsageState, 0)
	}
	return low
}

// Returns if the disk usage reaches high water mark w.r.t the configured cache quota.
// gc starts if high water mark reached.
func (c *diskCache) diskUsageHigh() bool {
	gcTriggerPct := c.quotaPct * c.highWatermark / 100
	di, err := disk.GetInfo(c.dir)
	if err != nil {
		reqInfo := (&logger.ReqInfo{}).AppendTags("cachePath", c.dir)
		ctx := logger.SetReqInfo(GlobalContext, reqInfo)
		logger.LogIf(ctx, err)
		return false
	}
	usedPercent := (di.Total - di.Free) * 100 / di.Total
	high := int(usedPercent) >= gcTriggerPct
	atomic.StoreUint64(&c.stats.UsagePercent, usedPercent)
	if high {
		atomic.StoreInt32(&c.stats.UsageState, 1)
	}
	return high
}

// Returns if size space can be allocated without exceeding
// max disk usable for caching
/*
 * 申请了 size 之后是否超过了 cache 的 quota
 */
func (c *diskCache) diskAvailable(size int64) bool {
	di, err := disk.GetInfo(c.dir)
	if err != nil {
		reqInfo := (&logger.ReqInfo{}).AppendTags("cachePath", c.dir)
		ctx := logger.SetReqInfo(GlobalContext, reqInfo)
		logger.LogIf(ctx, err)
		return false
	}
	usedPercent := (di.Total - (di.Free - uint64(size))) * 100 / di.Total
	return int(usedPercent) < c.quotaPct
}

// toClear returns how many bytes should be cleared to reach the low watermark quota.
// returns 0 if below quota.
func (c *diskCache) toClear() uint64 {
	di, err := disk.GetInfo(c.dir)
	if err != nil {
		reqInfo := (&logger.ReqInfo{}).AppendTags("cachePath", c.dir)
		ctx := logger.SetReqInfo(GlobalContext, reqInfo)
		logger.LogIf(ctx, err)
		return 0
	}
	return bytesToClear(int64(di.Total), int64(di.Free), uint64(c.quotaPct), uint64(c.lowWatermark))
}

var (
	errDoneForNow = errors.New("done for now")
)

func (c *diskCache) purgeWait(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
		case <-c.triggerGC: // wait here until someone triggers.
			c.purge(ctx)
		}
	}
}

// Purge cache entries that were not accessed.
/*
 * 1. 计算需要删除的数据量
 * 	删除多少能够达到 pct * lowwater
 * 2. 更新 gc 原子变量，defer 释放
 * 3. 开始遍历删除 cache 文件
 * 	3.1 用户自定义了 cache 策略
 * 		onlyIfCached = true 		表示永不过期
 * 		noStore = true 				表示不用cache
 * 		revalidated = true 			表示需要激活 cache 才能生效
 * 		sMaxAge、maxAge、minFresh 	表示有效时间，最后一个 mod 超过这个时间 则过期
 * 		expiry 						表示过期时间，maxStale + now 超过这个值，则过期
 * 										注意：跟 mod 时间没关系
 * 	3.2 用户没有定义 cache 策略，对文进行打分
 * 		3.2.1 (now - mod) * (1 + 0.25*szWeight + 0.25*hitsWeight)
 * 		3.2.2 szWeight = size * 1/toFree (0-1之间)
 * 		3.2.3 hitsWeight = 1 - hits / maxHits（100 固定值） (0-1之间)
 *	 注意：
 * 		1. 主要因素是最后一次访问时间距离现在有多久，最少占比 2/3
 * 		2. 其次因素是文件大小、命中次数占比一样，最多 1/6
 * 
 * 注意
 * 	1. 上述 gc 针对的一块磁盘的情况，cache 策略针对的是磁盘不是对象
 * 	2. 所以上述两种情况，只能运行一种，不能二者兼有
 */
func (c *diskCache) purge(ctx context.Context) {
	if atomic.LoadInt32(&c.purgeRunning) == 1 || c.diskUsageLow() {
		return
	}

	/*
	 * 1. 需要删除多少字节，能够达到 quotaPct * lowWatermark 以下
	 */
	toFree := c.toClear()
	if toFree == 0 {
		return
	}

	/*
	 * 2. 设置 gc 原子变量，defer 方式改回
	 */
	atomic.StoreInt32(&c.purgeRunning, 1) // do not run concurrent purge()
	defer atomic.StoreInt32(&c.purgeRunning, 0)

	// expiry for cleaning up old cache.json files that
	// need to be cleaned up.
	/*
	 * 90 天
	 */
	expiry := UTCNow().Add(-cacheExpiryDays)
	// defaulting max hits count to 100
	// ignore error we know what value we are passing.
	scorer, _ := newFileScorer(toFree, time.Now().Unix(), 100)

	// this function returns FileInfo for cached range files and cache data file.
	fiStatFn := func(ranges map[string]string, dataFile, pathPrefix string) map[string]os.FileInfo {
		fm := make(map[string]os.FileInfo)
		fname := pathJoin(pathPrefix, dataFile)
		if fi, err := os.Stat(fname); err == nil {
			fm[fname] = fi
		}

		for _, rngFile := range ranges {
			fname = pathJoin(pathPrefix, rngFile)
			if fi, err := os.Stat(fname); err == nil {
				fm[fname] = fi
			}
		}
		return fm
	}

	/*
	 * gc 的逻辑
	 */
	filterFn := func(name string, typ os.FileMode) error {
		if name == minioMetaBucket {
			// Proceed to next file.
			return nil
		}

		cacheDir := pathJoin(c.dir, name)
		meta, _, numHits, err := c.statCachedMeta(ctx, cacheDir)
		if err != nil {
			// delete any partially filled cache entry left behind.
			removeAll(cacheDir)
			// Proceed to next file.
			return nil
		}

		// stat all cached file ranges and cacheDataFile.
		cachedFiles := fiStatFn(meta.Ranges, cacheDataFile, pathJoin(c.dir, name))
		objInfo := meta.ToObjectInfo("", "")
		/*
		 * type cacheControl struct {
		 *		expiry       time.Time 	// 应该是过期时间 来自 objInfo
		 *		maxAge       int 		// 后面左右的选项均来自 userdefine
		 *		sMaxAge      int
		 *		minFresh     int
		 *		maxStale     int
		 *		noStore      bool
		 *		onlyIfCached bool
		 *		noCache      bool
		 *	}
		*/
		cc := cacheControlOpts(objInfo)
		for fname, fi := range cachedFiles {
			if cc != nil {
				/*
				 * 约束条件：
				 * 	onlyIfCached = true 表示永不过期
				 * 	noStore = true 		表示不用cache
				 * 	revalidated = true 	表示需要激活 cache 才能生效
				 * 	sMaxAge、maxAge、minFresh 表示有效时间，最后一个 mod 超过这个时间 则过期
				 * 	expiry 				表示过期时间，maxStale + now 超过这个值，则过期
				 * 						注意：跟 mod 时间没关系
				 */
				if cc.isStale(objInfo.ModTime) {
					if err = removeAll(fname); err != nil {
						logger.LogIf(ctx, err)
					}
					scorer.adjustSaveBytes(-fi.Size())

					// break early if sufficient disk space reclaimed.
					if c.diskUsageLow() {
						// if we found disk usage is already low, we return nil filtering is complete.
						return errDoneForNow
					}
				}
				continue
			}
			/*
			 * userdefine 内部没有 cache-control 这项时，加入 socer 的 queue 中
			 * queue 的插入顺序是根据 score 排序
			 * 排序方式：
			 * 	1. (now - mod) * (1 + 0.25*szWeight + 0.25*hitsWeight)
			 * 	2. szWeight = size * 1/toFree (0-1之间)
			 * 	3. hitsWeight = 1 - hits / maxHits（100 固定值） (0-1之间)
			 *
			 * 	最后一次修改之后的时间段作为基数
			 * 	文件越大越容易被淘汰
			 * 	被访问的次数越少越容易被淘汰
			 */
			scorer.addFile(fname, atime.Get(fi), fi.Size(), numHits)
		}
		// clean up stale cache.json files for objects that never got cached but access count was maintained in cache.json
		fi, err := os.Stat(pathJoin(cacheDir, cacheMetaJSONFile))
		/*
		 * 已过期（90天），删除目录
		 */
		if err != nil || (fi.ModTime().Before(expiry) && len(cachedFiles) == 0) {
			removeAll(cacheDir)
			scorer.adjustSaveBytes(-fi.Size())
			// Proceed to next file.
			return nil
		}

		// if we found disk usage is already low, we return nil filtering is complete.
		if c.diskUsageLow() {
			return errDoneForNow
		}

		// Proceed to next file.
		return nil
	}

	/*
	 * 遍历 cache 目录，回调函数 是 gc 逻辑
	 */
	if err := readDirFilterFn(c.dir, filterFn); err != nil {
		logger.LogIf(ctx, err)
		return
	}

	/*
	 * 从 queue 中取文件逐个删除
	 */
	scorer.purgeFunc(func(qfile queuedFile) {
		fileName := qfile.name
		removeAll(fileName)
		slashIdx := strings.LastIndex(fileName, SlashSeparator)
		if slashIdx >= 0 {
			fileNamePrefix := fileName[0:slashIdx]
			fname := fileName[slashIdx+1:]
			if fname == cacheDataFile {
				removeAll(fileNamePrefix)
			}
		}
	})

	scorer.reset()
}

// sets cache drive status
func (c *diskCache) setOffline() {
	atomic.StoreUint32(&c.online, 0)
}

// returns true if cache drive is online
func (c *diskCache) IsOnline() bool {
	return atomic.LoadUint32(&c.online) != 0
}

// Stat returns ObjectInfo from disk cache
func (c *diskCache) Stat(ctx context.Context, bucket, object string) (oi ObjectInfo, numHits int, err error) {
	var partial bool
	var meta *cacheMeta

	/*
	 * c.dir/SHA256Hash(bucket/object)
	 */
	cacheObjPath := getCacheSHADir(c.dir, bucket, object)
	// Stat the file to get file size.
	meta, partial, numHits, err = c.statCachedMeta(ctx, cacheObjPath)
	if err != nil {
		return
	}
	if partial {
		return oi, numHits, errFileNotFound
	}
	oi = meta.ToObjectInfo("", "")
	oi.Bucket = bucket
	oi.Name = object

	if err = decryptCacheObjectETag(&oi); err != nil {
		return
	}
	return
}

// statCachedMeta returns metadata from cache - including ranges cached, partial to indicate
// if partial object is cached.
func (c *diskCache) statCachedMeta(ctx context.Context, cacheObjPath string) (meta *cacheMeta, partial bool, numHits int, err error) {

	cLock := c.NewNSLockFn(ctx, cacheObjPath)
	if err = cLock.GetRLock(globalObjectTimeout); err != nil {
		return
	}

	defer cLock.RUnlock()
	return c.statCache(ctx, cacheObjPath)
}

// statRange returns ObjectInfo and RangeInfo from disk cache
func (c *diskCache) statRange(ctx context.Context, bucket, object string, rs *HTTPRangeSpec) (oi ObjectInfo, rngInfo RangeInfo, numHits int, err error) {
	// Stat the file to get file size.
	cacheObjPath := getCacheSHADir(c.dir, bucket, object)
	var meta *cacheMeta
	var partial bool

	/*
	 * 通过查看是否有 part.1 文件决定是否是 mutilpart数据
	 */
	meta, partial, numHits, err = c.statCachedMeta(ctx, cacheObjPath)
	if err != nil {
		return
	}

	oi = meta.ToObjectInfo("", "")
	oi.Bucket = bucket
	oi.Name = object
	if !partial {
		err = decryptCacheObjectETag(&oi)
		return
	}

	actualSize := uint64(meta.Stat.Size)
	var length int64
	_, length, err = rs.GetOffsetLength(int64(actualSize))
	if err != nil {
		return
	}

	actualRngSize := uint64(length)
	if globalCacheKMS != nil {
		actualRngSize, _ = sio.EncryptedSize(uint64(length))
	}

	rng := rs.String(int64(actualSize))
	rngFile, ok := meta.Ranges[rng]
	if !ok {
		return oi, rngInfo, numHits, ObjectNotFound{Bucket: bucket, Object: object}
	}
	if _, err = os.Stat(pathJoin(cacheObjPath, rngFile)); err != nil {
		return oi, rngInfo, numHits, ObjectNotFound{Bucket: bucket, Object: object}
	}
	rngInfo = RangeInfo{Range: rng, File: rngFile, Size: int64(actualRngSize)}

	err = decryptCacheObjectETag(&oi)
	return
}

// statCache is a convenience function for purge() to get ObjectInfo for cached object
func (c *diskCache) statCache(ctx context.Context, cacheObjPath string) (meta *cacheMeta, partial bool, numHits int, err error) {
	// Stat the file to get file size.
	/*
	 * cacheMetaJSONFile = cache.json
	 * cacheDataFile     = "part.1"
	 * cacheMetaVersion  = "1.0.0"
	 */
	metaPath := pathJoin(cacheObjPath, cacheMetaJSONFile)
	f, err := os.Open(metaPath)
	if err != nil {
		return meta, partial, 0, err
	}
	defer f.Close()

	/*
	 * type cacheMeta struct {
	 *		Version string   	// "1.0.0"
	 *		Stat    StatInfo 	// 包含 数据大小size 和 modtime
	 *		
	 *		Checksum CacheChecksumInfoV1 // 包含：校验和算法名，直接io 的数据块大小 4KB
	 *		Meta map[string]string 		 // 主要是 etag ，全部来自 userdefine
	 *		Ranges map[string]string 	 // 针对分片 cache，<offset-length, uuid(文件名)>
	 *		Hits int  					 // hit 技术
	 *	}
	 */
	meta = &cacheMeta{Version: cacheMetaVersion}
	if err := jsonLoad(f, meta); err != nil {
		return meta, partial, 0, err
	}
	// get metadata of part.1 if full file has been cached.
	partial = true
	fi, err := os.Stat(pathJoin(cacheObjPath, cacheDataFile))
	if err == nil {
		meta.Stat.ModTime = atime.Get(fi)
		partial = false
	}
	return meta, partial, meta.Hits, nil
}

// saves object metadata to disk cache
// incHitsOnly is true if metadata update is incrementing only the hit counter
func (c *diskCache) SaveMetadata(ctx context.Context, bucket, object string, meta map[string]string, actualSize int64, rs *HTTPRangeSpec, rsFileName string, incHitsOnly bool) error {
	cachedPath := getCacheSHADir(c.dir, bucket, object)
	cLock := c.NewNSLockFn(ctx, cachedPath)
	if err := cLock.GetLock(globalObjectTimeout); err != nil {
		return err
	}
	defer cLock.Unlock()
	return c.saveMetadata(ctx, bucket, object, meta, actualSize, rs, rsFileName, incHitsOnly)
}

// saves object metadata to disk cache
// incHitsOnly is true if metadata update is incrementing only the hit counter
func (c *diskCache) saveMetadata(ctx context.Context, bucket, object string, meta map[string]string, actualSize int64, rs *HTTPRangeSpec, rsFileName string, incHitsOnly bool) error {
	cachedPath := getCacheSHADir(c.dir, bucket, object)
	metaPath := pathJoin(cachedPath, cacheMetaJSONFile)
	// Create cache directory if needed
	if err := os.MkdirAll(cachedPath, 0777); err != nil {
		return err
	}
	f, err := os.OpenFile(metaPath, os.O_RDWR|os.O_CREATE, 0666)
	if err != nil {
		return err
	}
	defer f.Close()

	m := &cacheMeta{Version: cacheMetaVersion}
	if err := jsonLoad(f, m); err != nil && err != io.EOF {
		return err
	}
	// increment hits
	if rs != nil {
		// rsFileName gets set by putRange. Check for blank values here
		// coming from other code paths that set rs only (eg initial creation or hit increment).
		if rsFileName != "" {
			if m.Ranges == nil {
				m.Ranges = make(map[string]string)
			}
			m.Ranges[rs.String(actualSize)] = rsFileName
		}
	} else {
		// this is necessary cleanup of range files if entire object is cached.
		for _, f := range m.Ranges {
			removeAll(pathJoin(cachedPath, f))
		}
		m.Ranges = nil
	}
	m.Stat.Size = actualSize
	m.Stat.ModTime = UTCNow()
	if !incHitsOnly {
		// reset meta
		m.Meta = meta
	} else {
		if m.Meta == nil {
			m.Meta = make(map[string]string)
		}
		if etag, ok := meta["etag"]; ok {
			m.Meta["etag"] = etag
		}
	}

	m.Hits++

	m.Checksum = CacheChecksumInfoV1{Algorithm: HighwayHash256S.String(), Blocksize: cacheBlkSize}
	return jsonSave(f, m)
}

func getCacheSHADir(dir, bucket, object string) string {
	return pathJoin(dir, getSHA256Hash([]byte(pathJoin(bucket, object))))
}

// Cache data to disk with bitrot checksum added for each block of 1MB
func (c *diskCache) bitrotWriteToCache(cachePath, fileName string, reader io.Reader, size uint64) (int64, error) {
	if err := os.MkdirAll(cachePath, 0777); err != nil {
		return 0, err
	}
	filePath := pathJoin(cachePath, fileName)

	if filePath == "" || reader == nil {
		return 0, errInvalidArgument
	}

	if err := checkPathLength(filePath); err != nil {
		return 0, err
	}
	f, err := os.Create(filePath)
	if err != nil {
		return 0, osErrToFileErr(err)
	}
	defer f.Close()

	var bytesWritten int64

	h := HighwayHash256S.New()

	bufp := c.pool.Get().(*[]byte)
	defer c.pool.Put(bufp)

	var n, n2 int
	for {
		n, err = io.ReadFull(reader, *bufp)
		if err != nil && err != io.EOF && err != io.ErrUnexpectedEOF {
			return 0, err
		}
		eof := err == io.EOF || err == io.ErrUnexpectedEOF
		if n == 0 && size != 0 {
			// Reached EOF, nothing more to be done.
			break
		}
		h.Reset()
		if _, err = h.Write((*bufp)[:n]); err != nil {
			return 0, err
		}
		hashBytes := h.Sum(nil)
		if _, err = f.Write(hashBytes); err != nil {
			return 0, err
		}
		if n2, err = f.Write((*bufp)[:n]); err != nil {
			return 0, err
		}
		bytesWritten += int64(n2)
		if eof {
			break
		}
	}
	return bytesWritten, nil
}

func newCacheEncryptReader(content io.Reader, bucket, object string, metadata map[string]string) (r io.Reader, err error) {
	objectEncryptionKey, err := newCacheEncryptMetadata(bucket, object, metadata)
	if err != nil {
		return nil, err
	}

	reader, err := sio.EncryptReader(content, sio.Config{Key: objectEncryptionKey[:], MinVersion: sio.Version20})
	if err != nil {
		return nil, crypto.ErrInvalidCustomerKey
	}
	return reader, nil
}
func newCacheEncryptMetadata(bucket, object string, metadata map[string]string) ([]byte, error) {
	var sealedKey crypto.SealedKey
	if globalCacheKMS == nil {
		return nil, errKMSNotConfigured
	}
	key, encKey, err := globalCacheKMS.GenerateKey(globalCacheKMS.DefaultKeyID(), crypto.Context{bucket: pathJoin(bucket, object)})
	if err != nil {
		return nil, err
	}

	objectKey := crypto.GenerateKey(key, rand.Reader)
	sealedKey = objectKey.Seal(key, crypto.GenerateIV(rand.Reader), crypto.S3.String(), bucket, object)
	crypto.S3.CreateMetadata(metadata, globalCacheKMS.DefaultKeyID(), encKey, sealedKey)

	if etag, ok := metadata["etag"]; ok {
		metadata["etag"] = hex.EncodeToString(objectKey.SealETag([]byte(etag)))
	}
	metadata[SSECacheEncrypted] = ""
	return objectKey[:], nil
}

// Caches the object to disk
/*
 * 1. 如果超过 highwarter ，则通知 gc 协程进行 gc
 * 2. 获取全局的分布式锁，才能进行，主要是同步
 * 3. 如果没有该对象的 cache，则保存 meta 直接返回
 * 4. 如果有 cache_mate，但是还没有 cache，重新 save_meta 内部会 hit++
 * 	4.1 通过 etag 判断是否是同一个对象，像是用户自定义的唯一标识
 * 5. 如果是多分片数据（应该是 mutilpart），则进行 putRange
 * 	5.1 putRange 之后直接返回，后续逻辑是 cache 单独的 object
 * 	5.2 整体流程大致与单 object 的逻辑一致（bitrot 写入到一个指定的文件中）
 * 	5.3 文件 /cachedir/sha256hash(object/object)/uuid
 * 	5.4 meta 中 range 记录 <offset-length, uuid> 的映射
 * 6. 判断写入数据之后磁盘容量是否超过 pct * high_water
 * 7. 对数据进行加密部分的逻辑
 * 8. bitrot 写入 /cachedir/sha256hash(object/object)/part.1
 * 9. 重新 save_meta ，更新 hit
 *
 * 注意： cache_meta 是先有的，达到一定标准之后才 cache
 */
func (c *diskCache) Put(ctx context.Context, bucket, object string, data io.Reader, size int64, rs *HTTPRangeSpec, opts ObjectOptions, incHitsOnly bool) error {
	/*
	 * 1. 如果超过 highwarter ，则通知 gc 协程进行 gc
	 */
	if c.diskUsageHigh() {
		c.triggerGC <- struct{}{}
		io.Copy(ioutil.Discard, data)
		return errDiskFull
	}

	/*
	 * 2. 获取全局的分布式锁，才能进行，主要是同步
	 * 	疑问： 
	 * 		1. 这里面有一个问题其实很重要，就是每个节点 cache 的只有本地的数据吗？
	 * 		2. 如果可以跨节点 cache，那么每个节点的cache 的数据可能不一样，这个怎么解决的？
	 * 		3. 还有一个是，cache 的目录（cache 也是磁盘直接IO）必须是一个单独的挂在磁盘吗？
	 * 		4. 如果是本地节点cache 本地数据，都是直接 IO，cache 有用吗？
	 */
	cachePath := getCacheSHADir(c.dir, bucket, object)
	cLock := c.NewNSLockFn(ctx, cachePath)
	if err := cLock.GetLock(globalObjectTimeout); err != nil {
		return err
	}
	defer cLock.Unlock()

	meta, _, numHits, err := c.statCache(ctx, cachePath)
	// Case where object not yet cached
	/*
	 * 3. 如果没有该对象的 cache，则保存 meta 直接返回
	 * 4. 如果有 cache_mate，但是还没有 cache，重新 save_meta 内部会 hit++
	 * 	4.1 通过 etag 判断是否是同一个对象，像是用户自定义的唯一标识
	 *	注意：所以 cache_meta 是先有的，达到一定标准之后才 cache
	 */
	if os.IsNotExist(err) && c.after >= 1 {
		return c.saveMetadata(ctx, bucket, object, opts.UserDefined, size, nil, "", false)
	}
	// Case where object already has a cache metadata entry but not yet cached
	if err == nil && numHits < c.after {
		cETag := extractETag(meta.Meta)
		bETag := extractETag(opts.UserDefined)
		if cETag == bETag {
			return c.saveMetadata(ctx, bucket, object, opts.UserDefined, size, nil, "", false)
		}
		incHitsOnly = true
	}

	/* 
	 * 5. 如果是多分片数据（应该是 mutilpart），则进行 putRange
	 */
	if rs != nil {
		return c.putRange(ctx, bucket, object, data, size, rs, opts)
	}

	/*
	 * 6. 判断写入数据之后磁盘容量是否超过 pct * high_water
	 */
	if !c.diskAvailable(size) {
		return errDiskFull
	}
	if err := os.MkdirAll(cachePath, 0777); err != nil {
		return err
	}
	var metadata = make(map[string]string)
	for k, v := range opts.UserDefined {
		metadata[k] = v
	}
	var reader = data
	var actualSize = uint64(size)
	/*
	 * 7. 对数据进行加密解密部分的逻辑
	 */
	if globalCacheKMS != nil {
		reader, err = newCacheEncryptReader(data, bucket, object, metadata)
		if err != nil {
			return err
		}
		actualSize, _ = sio.EncryptedSize(uint64(size))
	}
	/*
	 * 8. bitrot 写入 /cachedir/sha256hash(object/object)/part.1
	 */
	n, err := c.bitrotWriteToCache(cachePath, cacheDataFile, reader, actualSize)
	if IsErr(err, baseErrs...) {
		// take the cache drive offline
		c.setOffline()
	}
	if err != nil {
		removeAll(cachePath)
		return err
	}

	if actualSize != uint64(n) {
		removeAll(cachePath)
		return IncompleteBody{}
	}
	/*
	 * 9. 重新 save_meta
	 */
	return c.saveMetadata(ctx, bucket, object, metadata, n, nil, "", incHitsOnly)
}

// Caches the range to disk
func (c *diskCache) putRange(ctx context.Context, bucket, object string, data io.Reader, size int64, rs *HTTPRangeSpec, opts ObjectOptions) error {
	rlen, err := rs.GetLength(size)
	if err != nil {
		return err
	}
	if !c.diskAvailable(rlen) {
		return errDiskFull
	}
	cachePath := getCacheSHADir(c.dir, bucket, object)
	if err := os.MkdirAll(cachePath, 0777); err != nil {
		return err
	}
	var metadata = make(map[string]string)
	for k, v := range opts.UserDefined {
		metadata[k] = v
	}
	var reader = data
	var actualSize = uint64(rlen)
	// objSize is the actual size of object (with encryption overhead if any)
	var objSize = uint64(size)
	if globalCacheKMS != nil {
		reader, err = newCacheEncryptReader(data, bucket, object, metadata)
		if err != nil {
			return err
		}
		actualSize, _ = sio.EncryptedSize(uint64(rlen))
		objSize, _ = sio.EncryptedSize(uint64(size))

	}
	cacheFile := MustGetUUID()
	n, err := c.bitrotWriteToCache(cachePath, cacheFile, reader, actualSize)
	if IsErr(err, baseErrs...) {
		// take the cache drive offline
		c.setOffline()
	}
	if err != nil {
		removeAll(cachePath)
		return err
	}
	if actualSize != uint64(n) {
		removeAll(cachePath)
		return IncompleteBody{}
	}
	return c.saveMetadata(ctx, bucket, object, metadata, int64(objSize), rs, cacheFile, false)
}

// checks streaming bitrot checksum of cached object before returning data
func (c *diskCache) bitrotReadFromCache(ctx context.Context, filePath string, offset, length int64, writer io.Writer) error {
	h := HighwayHash256S.New()

	checksumHash := make([]byte, h.Size())

	startBlock := offset / cacheBlkSize
	endBlock := (offset + length) / cacheBlkSize

	// get block start offset
	var blockStartOffset int64
	if startBlock > 0 {
		blockStartOffset = (cacheBlkSize + int64(h.Size())) * startBlock
	}

	tillLength := (cacheBlkSize + int64(h.Size())) * (endBlock - startBlock + 1)

	// Start offset cannot be negative.
	if offset < 0 {
		logger.LogIf(ctx, errUnexpected)
		return errUnexpected
	}

	// Writer cannot be nil.
	if writer == nil {
		logger.LogIf(ctx, errUnexpected)
		return errUnexpected
	}
	var blockOffset, blockLength int64
	rc, err := readCacheFileStream(filePath, blockStartOffset, tillLength)
	if err != nil {
		return err
	}
	bufp := c.pool.Get().(*[]byte)
	defer c.pool.Put(bufp)

	for block := startBlock; block <= endBlock; block++ {
		switch {
		case startBlock == endBlock:
			blockOffset = offset % cacheBlkSize
			blockLength = length
		case block == startBlock:
			blockOffset = offset % cacheBlkSize
			blockLength = cacheBlkSize - blockOffset
		case block == endBlock:
			blockOffset = 0
			blockLength = (offset + length) % cacheBlkSize
		default:
			blockOffset = 0
			blockLength = cacheBlkSize
		}
		if blockLength == 0 {
			break
		}
		if _, err := io.ReadFull(rc, checksumHash); err != nil {
			return err
		}
		h.Reset()
		n, err := io.ReadFull(rc, *bufp)
		if err != nil && err != io.EOF && err != io.ErrUnexpectedEOF {
			logger.LogIf(ctx, err)
			return err
		}
		eof := err == io.EOF || err == io.ErrUnexpectedEOF
		if n == 0 && length != 0 {
			// Reached EOF, nothing more to be done.
			break
		}

		if _, e := h.Write((*bufp)[:n]); e != nil {
			return e
		}
		hashBytes := h.Sum(nil)

		if !bytes.Equal(hashBytes, checksumHash) {
			err = fmt.Errorf("hashes do not match expected %s, got %s",
				hex.EncodeToString(checksumHash), hex.EncodeToString(hashBytes))
			logger.LogIf(GlobalContext, err)
			return err
		}

		if _, err := io.Copy(writer, bytes.NewReader((*bufp)[blockOffset:blockOffset+blockLength])); err != nil {
			if err != io.ErrClosedPipe {
				logger.LogIf(ctx, err)
				return err
			}
			eof = true
		}
		if eof {
			break
		}
	}

	return nil
}

// Get returns ObjectInfo and reader for object from disk cache
/*
 * 0. 分布式锁
 * 1. 根据是否是存在 part.1 文件判断是否是分片数据，组织成 objInfo
 * 2. 如果是分片数据，则根据 size 计算出是哪个 rang 的文件，组织成 rngInfo
 * 3. 针对目录对象进行处理，没有读数据部分，直接返回
 * 4. 新的协程异步 bitrot 读
 */
func (c *diskCache) Get(ctx context.Context, bucket, object string, rs *HTTPRangeSpec, h http.Header, opts ObjectOptions) (gr *GetObjectReader, numHits int, err error) {
	cacheObjPath := getCacheSHADir(c.dir, bucket, object)
	/*
	 * 0. 分布式锁
	 */
	cLock := c.NewNSLockFn(ctx, cacheObjPath)
	if err := cLock.GetRLock(globalObjectTimeout); err != nil {
		return nil, numHits, err
	}

	defer cLock.RUnlock()
	var objInfo ObjectInfo
	var rngInfo RangeInfo
	/*
	 * 1. 根据是否是存在 part.1 文件判断是否是分片数据，组织成 objInfo
	 * 2. 如果是分片数据，则根据 size 计算出是哪个 rang 的文件，组织成 rngInfo
	 */
	if objInfo, rngInfo, numHits, err = c.statRange(ctx, bucket, object, rs); err != nil {
		return nil, numHits, toObjectErr(err, bucket, object)
	}
	cacheFile := cacheDataFile
	objSize := objInfo.Size
	if !rngInfo.Empty() {
		// for cached ranges, need to pass actual range file size to GetObjectReader
		// and clear out range spec
		cacheFile = rngInfo.File
		objInfo.Size = rngInfo.Size
		rs = nil
	}
	var nsUnlocker = func() {}
	// For a directory, we need to send an reader that returns no bytes.
	/*
	 * 3. 针对目录对象处理
	 */
	if HasSuffix(object, SlashSeparator) {
		// The lock taken above is released when
		// objReader.Close() is called by the caller.
		gr, gerr := NewGetObjectReaderFromReader(bytes.NewBuffer(nil), objInfo, opts, nsUnlocker)
		return gr, numHits, gerr
	}

	fn, off, length, nErr := NewGetObjectReader(rs, objInfo, opts, nsUnlocker)
	if nErr != nil {
		return nil, numHits, nErr
	}
	filePath := pathJoin(cacheObjPath, cacheFile)
	pr, pw := io.Pipe()
	/*
	 * 4. 新的协程异步 bitrot 读
	 */
	go func() {
		err := c.bitrotReadFromCache(ctx, filePath, off, length, pw)
		if err != nil {
			removeAll(cacheObjPath)
		}
		pw.CloseWithError(err)
	}()
	// Cleanup function to cause the go routine above to exit, in
	// case of incomplete read.
	pipeCloser := func() { pr.Close() }

	gr, gerr := fn(pr, h, opts.CheckPrecondFn, pipeCloser)
	if gerr != nil {
		return gr, numHits, gerr
	}
	if globalCacheKMS != nil {
		// clean up internal SSE cache metadata
		delete(gr.ObjInfo.UserDefined, crypto.SSEHeader)
	}
	if !rngInfo.Empty() {
		// overlay Size with actual object size and not the range size
		gr.ObjInfo.Size = objSize
	}
	return gr, numHits, nil

}

// Deletes the cached object
func (c *diskCache) delete(ctx context.Context, cacheObjPath string) (err error) {
	cLock := c.NewNSLockFn(ctx, cacheObjPath)
	if err := cLock.GetLock(globalObjectTimeout); err != nil {
		return err
	}
	defer cLock.Unlock()
	return removeAll(cacheObjPath)
}

// Deletes the cached object
func (c *diskCache) Delete(ctx context.Context, bucket, object string) (err error) {
	cacheObjPath := getCacheSHADir(c.dir, bucket, object)
	return c.delete(ctx, cacheObjPath)
}

// convenience function to check if object is cached on this diskCache
func (c *diskCache) Exists(ctx context.Context, bucket, object string) bool {
	if _, err := os.Stat(getCacheSHADir(c.dir, bucket, object)); err != nil {
		return false
	}
	return true
}
