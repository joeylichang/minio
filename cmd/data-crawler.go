/*
 * MinIO Cloud Storage, (C) 2020 MinIO, Inc.
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
	"encoding/binary"
	"errors"
	"os"
	"path"
	"strconv"
	"strings"
	"time"

	"github.com/minio/minio/cmd/config"
	"github.com/minio/minio/cmd/logger"
	"github.com/minio/minio/pkg/bucket/lifecycle"
	"github.com/minio/minio/pkg/bucket/replication"
	"github.com/minio/minio/pkg/color"
	"github.com/minio/minio/pkg/env"
	"github.com/minio/minio/pkg/event"
	"github.com/minio/minio/pkg/hash"
	"github.com/willf/bloom"
)

const (
	dataCrawlSleepPerFolder  = time.Millisecond // Time to wait between folders.
	dataCrawlSleepDefMult    = 10.0             // Default multiplier for waits between operations.
	dataCrawlStartDelay      = 5 * time.Minute  // Time to wait on startup and between cycles.
	dataUsageUpdateDirCycles = 16               // Visit all folders every n cycles.

)

// initDataCrawler will start the crawler unless disabled.
func initDataCrawler(ctx context.Context, objAPI ObjectLayer) {
	if env.Get(envDataUsageCrawlConf, config.EnableOn) == config.EnableOn {
		go runDataCrawler(ctx, objAPI)
	}
}

// runDataCrawler will start a data crawler.
// The function will block until the context is canceled.
// There should only ever be one crawler running per cluster.
/*
 * 主要两个功能：
 * 	1. 收集 DataUsage 信息，写入 /diskpath/.minio.sys/buckets/.usage.json
 * 		1.1 遍历 zone、set，选一块磁盘，获取其上全部的 bucket
 * 		1.2 遍历 zone、set，加载 /diskpath/.minio.sys/buckets/.usage-cache.bin DataUsage数据
 * 		1.3 根据记载的 DataUsage，更新部分数据，然后调用 crawlDataFolder 扫描数据进行收集 DataUsage
 * 			1.3.1 每块磁盘都会 调用 crawlDataFolder 并且落盘
 * 			1.3.2 然后生成的结果通过管道向上层返回，然后在 Set 级别进行汇总
 * 			1.3.3 汇总的结果是给下一步 Quta 使用，并且通过管道 PutObject /diskpath/.minio.sys/buckets/.usage.json
 * 		1.4 根据 FIFOQuta，删除超过额定大小的对象数据
 * 	2. 周期性的更新 boolmfilter
 * 		2.1 5min 一个周期更次年一次 boolfilter
 * 		2.2 存储在 .minio.sys/buckets/.tracker.bin
 * 		2.3 intDataUpdateTracker 有两个协程
 * 			2.3.1 一个是接收来自管道的 bucket/object 数据添加到当前的 filter
 * 			2.3.2 一个是接收来自管道的 消息进行落盘（leader 发送过来的请求）
 * 		2.4 记录下一个的 filter 号，putObject 到 .minio.sys/buckets/.bloomcycle.bin
 */
func runDataCrawler(ctx context.Context, objAPI ObjectLayer) {
	// Load current bloom cycle
	nextBloomCycle := intDataUpdateTracker.current() + 1
	var buf bytes.Buffer
	/*
	 *  .minio.sys/buckets/.bloomcycle.bin 
	 */
	err := objAPI.GetObject(ctx, dataUsageBucket, dataUsageBloomName, 0, -1, &buf, "", ObjectOptions{})
	if err != nil {
		if !isErrObjectNotFound(err) && !isErrBucketNotFound(err) {
			logger.LogIf(ctx, err)
		}
	} else {
		if buf.Len() == 8 {
			nextBloomCycle = binary.LittleEndian.Uint64(buf.Bytes())
		}
	}

	for {  
		select {
		case <-ctx.Done():
			return
		/*
		 * 5 min
		 */
		case <-time.NewTimer(dataCrawlStartDelay).C:
			// Wait before starting next cycle and wait on startup.
			results := make(chan DataUsageInfo, 1)
			/*
			 * 等待 DataUsage 数据，然后写入如下对象中
			 * /diskpath/.minio.sys/buckets/.usage.json
			 */
			go storeDataUsageInBackend(ctx, objAPI, results)
			/*
			 * 1. for 循环之外是，每重启一次需要更新一下 filter
			 * 2. 每个周期更新一次 filter
			 * 3. 保留最近 16个 history
			 * 4. 每次更新都会将 nextBloomCycle 存储在 .minio.sys/buckets/.bloomcycle.bin
			 *
			 * CrawlAndGetDataUsage
			 * 	1. 收集并落盘，但是并没有在这层收集结果，并做结果进行操作
			 * 	2. 根据 FIFOQuta，删除超过额定大小的对象数据
			 */
			bf, err := globalNotificationSys.updateBloomFilter(ctx, nextBloomCycle)
			logger.LogIf(ctx, err)
			err = objAPI.CrawlAndGetDataUsage(ctx, bf, results)
			close(results)
			logger.LogIf(ctx, err)
			if err == nil {
				// Store new cycle...
				nextBloomCycle++
				/*
				 * dataUpdateTrackerResetEvery = 1000
				 */
				if nextBloomCycle%dataUpdateTrackerResetEvery == 0 {
					if intDataUpdateTracker.debug {
						logger.Info(color.Green("runDataCrawler:") + " Resetting bloom filter for next runs.")
					}
					nextBloomCycle++
				}
				var tmp [8]byte
				binary.LittleEndian.PutUint64(tmp[:], nextBloomCycle)
				r, err := hash.NewReader(bytes.NewReader(tmp[:]), int64(len(tmp)), "", "", int64(len(tmp)), false)
				if err != nil {
					logger.LogIf(ctx, err)
					continue
				}

				_, err = objAPI.PutObject(ctx, dataUsageBucket, dataUsageBloomName, NewPutObjReader(r, nil, nil), ObjectOptions{})
				if !isErrBucketNotFound(err) {
					logger.LogIf(ctx, err)
				}
			}
		}
	}
}

type cachedFolder struct {
	name   string
	parent *dataUsageHash
}

type folderScanner struct {
	root               string
	getSize            getSizeFn
	oldCache           dataUsageCache
	newCache           dataUsageCache
	withFilter         *bloomFilter
	waitForLowActiveIO func()

	dataUsageCrawlMult  float64
	dataUsageCrawlDebug bool

	newFolders      []cachedFolder
	existingFolders []cachedFolder
}

// crawlDataFolder will crawl the basepath+cache.Info.Name and return an updated cache.
// The returned cache will always be valid, but may not be updated from the existing.
// Before each operation waitForLowActiveIO is called which can be used to temporarily halt the crawler.
// If the supplied context is canceled the function will return at the first chance.
/*
 * 1. dataUsageCache 统计数据
 * 2. filter、rule 这两个对统计数据的影响是，有的话需要重新算，否则直接替换，这个要在后面理解一下是为什么？
 */
func crawlDataFolder(ctx context.Context, basePath string, cache dataUsageCache, waitForLowActiveIO func(), getSize getSizeFn) (dataUsageCache, error) {
	t := UTCNow()

	logPrefix := color.Green("data-usage: ")
	logSuffix := color.Blue(" - %v + %v", basePath, cache.Info.Name)
	if intDataUpdateTracker.debug {
		defer func() {
			logger.Info(logPrefix+" Crawl time: %v"+logSuffix, time.Since(t))
		}()

	}

	switch cache.Info.Name {
	case "", dataUsageRoot:
		return cache, errors.New("internal error: root scan attempted")
	}

	delayMult, err := strconv.ParseFloat(env.Get(envDataUsageCrawlDelay, "10.0"), 64)
	if err != nil {
		logger.LogIf(ctx, err)
		delayMult = dataCrawlSleepDefMult
	}

	s := folderScanner{
		root:                basePath,
		getSize:             getSize,
		oldCache:            cache,
		newCache:            dataUsageCache{Info: cache.Info},
		waitForLowActiveIO:  waitForLowActiveIO,
		newFolders:          nil,
		existingFolders:     nil,
		dataUsageCrawlMult:  delayMult,
		dataUsageCrawlDebug: intDataUpdateTracker.debug,
	}

	if len(cache.Info.BloomFilter) > 0 {
		s.withFilter = &bloomFilter{BloomFilter: &bloom.BloomFilter{}}
		_, err := s.withFilter.ReadFrom(bytes.NewBuffer(cache.Info.BloomFilter))
		if err != nil {
			logger.LogIf(ctx, err, logPrefix+"Error reading bloom filter")
			s.withFilter = nil
		}
	}
	if s.dataUsageCrawlDebug {
		logger.Info(logPrefix+"Start crawling. Bloom filter: %v"+logSuffix, s.withFilter != nil)
	}

	done := ctx.Done()
	var flattenLevels = 2

	if s.dataUsageCrawlDebug {
		logger.Info(logPrefix+"Cycle: %v, Entries: %v"+logSuffix, cache.Info.NextCycle, len(cache.Cache))
	}

	// Always scan flattenLevels deep. Cache root is level 0.
	todo := []cachedFolder{{name: cache.Info.Name}}
	/* 
	 * 1. 遍历 2 层 
	 * 2. 应该是 cache 了 object 及其子项
	 * 3. cache 的内容应该也不是用户数据，而是元数据，bucket 和 object 目录下的文件或者目录的元数据 
	 * 
	 * scanQueuedLevels
	 * 	目录：没有更新任何cache（old、new），更新了 newFolders、existingFolders
	 * 	文件：更新 newCache
	 */
	for i := 0; i < flattenLevels; i++ {
		if s.dataUsageCrawlDebug {
			logger.Info(logPrefix+"Level %v, scanning %v directories."+logSuffix, i, len(todo))
		}
		select {
		case <-done:
			return cache, ctx.Err()
		default:
		}
		var err error
		todo, err = s.scanQueuedLevels(ctx, todo, i == flattenLevels-1)
		if err != nil {
			// No useful information...
			return cache, err
		}
	}

	if s.dataUsageCrawlDebug {
		logger.Info(logPrefix+"New folders: %v"+logSuffix, s.newFolders)
	}

	// Add new folders first
	/*
	 * 针对新目录
	 * deepScanFolder，主逻辑大致与 scanQueuedLevels 相同
	 * 更新 cache 成员变量（递归遍历了所有的子项）：
	 * 	cache.Size 
	 *	cache.Objects
	 *	cache.ObjSizes
	 * 然后将 cache 加入到 newCache
	 */
	for _, folder := range s.newFolders {
		select {
		case <-done:
			return s.newCache, ctx.Err()
		default:
		}
		du, err := s.deepScanFolder(ctx, folder.name)
		if err != nil {
			logger.LogIf(ctx, err)
			continue
		}
		if du == nil {
			logger.Info(logPrefix + "no disk usage provided" + logSuffix)
			continue
		}

		s.newCache.replace(folder.name, "", *du)
		// Add to parent manually
		if folder.parent != nil {
			parent := s.newCache.Cache[folder.parent.Key()]
			parent.addChildString(folder.name)
		}
	}

	if s.dataUsageCrawlDebug {
		logger.Info(logPrefix+"Existing folders: %v"+logSuffix, len(s.existingFolders))
	}

	// Do selective scanning of existing folders.
	/*
	 * 针对旧目录
	 * 1. 规律约束
	 * 	1.1 下面 hash 部分逻辑
	 * 	1.2 目录没有 ActiveRules 且 没有 withFilter，则直接加入 newcache
	 * 2. deepScanFolder 逻辑
	 */
	for _, folder := range s.existingFolders {
		select {
		case <-done:
			return s.newCache, ctx.Err()
		default:
		}
		h := hashPath(folder.name)
		/*
		 * hash(h) % dataUsageUpdateDirCycles != s.oldCache.Info.NextCycle % dataUsageUpdateDirCycles
		 * dataUsageUpdateDirCycles == 16
		 * 这种逻辑有一定的巧合吧？有的目录满足有的目录不满足，与 dataUsageUpdateDirCycles 没关系啊
		 */
		if !h.mod(s.oldCache.Info.NextCycle, dataUsageUpdateDirCycles) {
			s.newCache.replaceHashed(h, folder.parent, s.oldCache.Cache[h.Key()])
			continue
		}

		/*
		 * 目录没有 ActiveRules 且 没有 withFilter，则直接加入 newcache
		 */
		if s.withFilter != nil {
			_, prefix := path2BucketObjectWithBasePath(basePath, folder.name)
			if s.oldCache.Info.lifeCycle == nil || !s.oldCache.Info.lifeCycle.HasActiveRules(prefix, true) {
				// If folder isn't in filter, skip it completely.
				if !s.withFilter.containsDir(folder.name) {
					if s.dataUsageCrawlDebug {
						logger.Info(logPrefix+"Skipping non-updated folder: %v"+logSuffix, folder)
					}
					s.newCache.replaceHashed(h, folder.parent, s.oldCache.Cache[h.Key()])
					continue
				}
			}
		}

		// Update on this cycle...
		du, err := s.deepScanFolder(ctx, folder.name)
		if err != nil {
			logger.LogIf(ctx, err)
			continue
		}
		if du == nil {
			logger.LogIf(ctx, errors.New("data-usage: no disk usage provided"))
			continue
		}
		s.newCache.replaceHashed(h, folder.parent, *du)
	}
	if s.dataUsageCrawlDebug {
		logger.Info(logPrefix+"Finished crawl, %v entries"+logSuffix, len(s.newCache.Cache))
	}
	s.newCache.Info.LastUpdate = UTCNow()
	s.newCache.Info.NextCycle++
	return s.newCache, nil
}

// scanQueuedLevels will scan the provided folders.
// Files found in the folders will be added to f.newCache.
// If final is provided folders will be put into f.newFolders or f.existingFolders.
// If final is not provided the folders found are returned from the function.
/*
 * 作用： 更新 folderScanner 成员变量，重点在 existingFolders、newFolders、newCache
 * 逻辑：
 * 	1. activeLifeCycle 与 filter
 * 		1.1 有 activeLifeCycle ，走正常逻辑
 * 		1.2 没有 activeLifeCycle，只有 filter，则直接 从oldCache copy 到 newCache（递归子目录）
 * 	 注意：因为 activeLifeCycle 有一些过期等策略，可能涉及到删除啊，计算大小的变化
 * 	2. 流控
 * 		2.1 waitForLowActiveIO 流控 3个并发，之后 sleep 10ms
 * 	3. 遍历目录线面的子项（可能是目录，可能是文件），更新 folderScanner 子项
 * 		f.existingFolders：子项是目录即不是文件，在 oldCache 中已经存在
 * 		f.newFolders：子项是目录即不是文件，新增的节点
 * 		f.newCache：子项是文件，crawlDataFolder 传入的回调计算实际大小，更新 cache，然后用cache 替换（或者添加） newCache
 * 			cache.Size ，object 目录下的子项的实际大小
			cache.Objects，object 目录下的子项的个数
			cache.ObjSizes，Size的数组
 * 	4. 返回值
 * 		final == false 时，将节点加入 []cachedFolder，用于下一轮遍历
 */
func (f *folderScanner) scanQueuedLevels(ctx context.Context, folders []cachedFolder, final bool) ([]cachedFolder, error) {
	var nextFolders []cachedFolder
	done := ctx.Done()
	for _, folder := range folders {
		select {
		case <-done:
			return nil, ctx.Err()
		default:
		}
		/*
		 * 路径去掉分隔符"/"，之后的字符串，hash 值
		 */
		thisHash := hashPath(folder.name)

		/*
		 * 如果 oldCache 有 active 的 LifeCycle ，且是这个目录的前缀，则 filter 置为 nil
		 */
		// If there are lifecycle rules for the prefix, remove the filter.
		filter := f.withFilter
		var activeLifeCycle *lifecycle.Lifecycle
		if f.oldCache.Info.lifeCycle != nil && filter != nil {
			_, prefix := path2BucketObjectWithBasePath(f.root, folder.name)
			if f.oldCache.Info.lifeCycle.HasActiveRules(prefix, true) {
				if f.dataUsageCrawlDebug {
					logger.Info(color.Green("data-usage:")+" Prefix %q has active rules", prefix)
				}
				activeLifeCycle = f.oldCache.Info.lifeCycle
				filter = nil
			}
		}
		/*
		 * 如果 oldCache 有这个路径的 cache 且 filter 不为空（说明没有 active 的 LifeCycle）
		 * 并且，不是 dataUsageRoot（"/"），且 filter 不包括该路径，则递归将 oldcache copy（子目录） 到 newCache
		 */
		if _, ok := f.oldCache.Cache[thisHash.Key()]; filter != nil && ok {
			// If folder isn't in filter and we have data, skip it completely.
			if folder.name != dataUsageRoot && !filter.containsDir(folder.name) {
				f.newCache.copyWithChildren(&f.oldCache, thisHash, folder.parent)
				if f.dataUsageCrawlDebug {
					logger.Info(color.Green("data-usage:")+" Skipping non-updated folder: %v", folder.name)
				}
				continue
			}
		}
		f.waitForLowActiveIO()
		/*
		 * sleep 10 ms 
		 */
		sleepDuration(dataCrawlSleepPerFolder, f.dataUsageCrawlMult)

		cache := dataUsageEntry{}

		/*
		 * readDirFn 遍历一个目录下面的内容（可能是文件、可能是目录），追个调用回调 func
		 */
		err := readDirFn(path.Join(f.root, folder.name), func(entName string, typ os.FileMode) error {
			// Parse
			entName = path.Clean(path.Join(folder.name, entName))
			bucket, prefix := path2BucketObjectWithBasePath(f.root, entName)
			if bucket == "" {
				if f.dataUsageCrawlDebug {
					logger.Info(color.Green("data-usage:")+" no bucket (%s,%s)", f.root, entName)
				}
				return nil
			}

			/*
			 * 1. 是否满足 s3 命名标准
			 * 2. 是否是系统保留的 minio、minio.sys
			 */
			if isReservedOrInvalidBucket(bucket, false) {
				if f.dataUsageCrawlDebug {
					logger.Info(color.Green("data-usage:")+" invalid bucket: %v, entry: %v", bucket, entName)
				}
				return nil
			}

			select {
			case <-done:
				return ctx.Err()
			default:
			}

			/*
			 * 目录
			 */
			if typ&os.ModeDir != 0 {
				h := hashPath(entName)
				_, exists := f.oldCache.Cache[h.Key()]
				cache.addChildString(entName)

				this := cachedFolder{name: entName, parent: &thisHash}
				cache.addChild(h)
				if final {
					if exists {
						f.existingFolders = append(f.existingFolders, this)
					} else {
						f.newFolders = append(f.newFolders, this)
					}
				} else {
					/*
					 * 下一轮需要继续遍历的
					 */
					nextFolders = append(nextFolders, this)
				}
				return nil
			}
			f.waitForLowActiveIO()
			// Dynamic time delay.
			t := UTCNow()

			// Get file size, ignore errors.
			item := crawlItem{
				Path:       path.Join(f.root, entName),
				Typ:        typ,
				bucket:     bucket,
				prefix:     path.Dir(prefix),
				objectName: path.Base(entName),
				debug:      f.dataUsageCrawlDebug,
				lifeCycle:  activeLifeCycle,
			}
			size, err := f.getSize(item)

			sleepDuration(time.Since(t), f.dataUsageCrawlMult)
			if err == errSkipFile || err == errFileNotFound {
				return nil
			}
			logger.LogIf(ctx, err)
			cache.Size += size
			cache.Objects++
			cache.ObjSizes.add(size)

			return nil
		})
		if err != nil {
			return nil, err
		}
		f.newCache.replaceHashed(thisHash, folder.parent, cache)
	}
	return nextFolders, nil
}

// deepScanFolder will deep scan a folder and return the size if no error occurs.
func (f *folderScanner) deepScanFolder(ctx context.Context, folder string) (*dataUsageEntry, error) {
	var cache dataUsageEntry

	done := ctx.Done()

	var addDir func(entName string, typ os.FileMode) error
	var dirStack = []string{f.root, folder}

	addDir = func(entName string, typ os.FileMode) error {
		select {
		case <-done:
			return ctx.Err()
		default:
		}

		f.waitForLowActiveIO()
		/*
		 * 递归调用，知道到文件
		 */
		if typ&os.ModeDir != 0 {
			dirStack = append(dirStack, entName)
			err := readDirFn(path.Join(dirStack...), addDir)
			dirStack = dirStack[:len(dirStack)-1]
			sleepDuration(dataCrawlSleepPerFolder, f.dataUsageCrawlMult)
			return err
		}

		// Dynamic time delay.
		t := UTCNow()

		// Get file size, ignore errors.
		dirStack = append(dirStack, entName)
		fileName := path.Join(dirStack...)
		dirStack = dirStack[:len(dirStack)-1]

		bucket, prefix := path2BucketObjectWithBasePath(f.root, fileName)
		var activeLifeCycle *lifecycle.Lifecycle
		if f.oldCache.Info.lifeCycle != nil {
			if f.oldCache.Info.lifeCycle.HasActiveRules(prefix, false) {
				if f.dataUsageCrawlDebug {
					logger.Info(color.Green("data-usage:")+" Prefix %q has active rules", prefix)
				}
				activeLifeCycle = f.oldCache.Info.lifeCycle
			}
		}

		size, err := f.getSize(
			crawlItem{
				Path:       fileName,
				Typ:        typ,
				bucket:     bucket,
				prefix:     path.Dir(prefix),
				objectName: path.Base(entName),
				debug:      f.dataUsageCrawlDebug,
				lifeCycle:  activeLifeCycle,
			})

		// Don't sleep for really small amount of time
		sleepDuration(time.Since(t), f.dataUsageCrawlMult)

		if err == errSkipFile {
			return nil
		}
		logger.LogIf(ctx, err)
		cache.Size += size
		cache.Objects++
		cache.ObjSizes.add(size)
		return nil
	}
	err := readDirFn(path.Join(dirStack...), addDir)
	if err != nil {
		return nil, err
	}
	return &cache, nil
}

// crawlItem represents each file while walking.
type crawlItem struct {
	Path string
	Typ  os.FileMode

	bucket     string // Bucket.
	prefix     string // Only the prefix if any, does not have final object name.
	objectName string // Only the object name without prefixes.
	lifeCycle  *lifecycle.Lifecycle
	debug      bool
}

type getSizeFn func(item crawlItem) (int64, error)

// transformMetaDir will transform a directory to prefix/file.ext
func (i *crawlItem) transformMetaDir() {
	split := strings.Split(i.prefix, SlashSeparator)
	if len(split) > 1 {
		i.prefix = path.Join(split[:len(split)-1]...)
	} else {
		i.prefix = ""
	}
	// Object name is last element
	i.objectName = split[len(split)-1]
}

// actionMeta contains information used to apply actions.
type actionMeta struct {
	oi          ObjectInfo
	trustOI     bool // Set true if oi can be trusted and has been read with quorum.
	numVersions int  // The number of versions of this object
}

// applyActions will apply lifecycle checks on to a scanned item.
// The resulting size on disk will always be returned.
// The metadata will be compared to consensus on the object layer before any changes are applied.
// If no metadata is supplied, -1 is returned if no action is taken.
func (i *crawlItem) applyActions(ctx context.Context, o ObjectLayer, meta actionMeta) (size int64) {
	/*
	 * object 的实际大小（解压缩或者解密之后的大小）
	 */
	size, err := meta.oi.GetActualSize()
	if i.debug {
		logger.LogIf(ctx, err)
	}
	/*
	 * lifeCycle 应该是没有一些约束（前缀、后缀、过期、与或并之类的），直接返回实际大小
	 */
	if i.lifeCycle == nil {
		return size
	}

	versionID := meta.oi.VersionID
	/*
	 * 根据 lifeCycle rule 计算 object 是否删除
	 * 返回三种：DeleteVersionAction、DeleteAction、NoneAction
	 */
	action := i.lifeCycle.ComputeAction(
		lifecycle.ObjectOpts{
			Name:         i.objectPath(),
			UserTags:     meta.oi.UserTags,
			ModTime:      meta.oi.ModTime,
			VersionID:    meta.oi.VersionID,
			DeleteMarker: meta.oi.DeleteMarker,
			IsLatest:     meta.oi.IsLatest,
			NumVersions:  meta.numVersions,
		})
	if i.debug {
		logger.Info(color.Green("applyActions:")+" lifecycle: %q, Initial scan: %v", i.objectPath(), action)
	}
	switch action {
	case lifecycle.DeleteAction, lifecycle.DeleteVersionAction:
	default:
		// No action.
		return size
	}

	// These (expensive) operations should only run on items we are likely to delete.
	// Load to ensure that we have the correct version and not an unsynced version.
	/*
	 * 对于 要删除，且是非 trustOI 的情况，重新计算一次
	 * ObjectInfo 不再是传入的参数，而是重新获取（像是一些同步的原因后面再看）
	 *
	 */
	if !meta.trustOI {
		obj, err := o.GetObjectInfo(ctx, i.bucket, i.objectPath(), ObjectOptions{
			VersionID: versionID,
		})
		if err != nil {
			switch err.(type) {
			case MethodNotAllowed: // This happens usually for a delete marker
				if !obj.DeleteMarker { // if this is not a delete marker log and return
					// Do nothing - heal in the future.
					logger.LogIf(ctx, err)
					return size
				}
			case ObjectNotFound:
				// object not found return 0
				return 0
			default:
				// All other errors proceed.
				logger.LogIf(ctx, err)
				return size
			}
		}
		size = obj.Size

		// Recalculate action.
		action = i.lifeCycle.ComputeAction(
			lifecycle.ObjectOpts{
				Name:         i.objectPath(),
				UserTags:     obj.UserTags,
				ModTime:      obj.ModTime,
				VersionID:    obj.VersionID,
				DeleteMarker: obj.DeleteMarker,
				IsLatest:     obj.IsLatest,
				NumVersions:  meta.numVersions,
			})
		if i.debug {
			logger.Info(color.Green("applyActions:")+" lifecycle: Secondary scan: %v", action)
		}
		versionID = obj.VersionID
		switch action {
		case lifecycle.DeleteAction, lifecycle.DeleteVersionAction:
		default:
			// No action.
			return size
		}
	}

	/*
	 * 删除 object
	 */
	opts := ObjectOptions{}
	switch action {
	case lifecycle.DeleteVersionAction:
		opts.VersionID = versionID
	case lifecycle.DeleteAction:
		opts.Versioned = globalBucketVersioningSys.Enabled(i.bucket)
	}

	obj, err := o.DeleteObject(ctx, i.bucket, i.objectPath(), opts)
	if err != nil {
		// Assume it is still there.
		logger.LogIf(ctx, err)
		return size
	}

	// Notify object deleted event.
	sendEvent(eventArgs{
		EventName:  event.ObjectRemovedDelete,
		BucketName: i.bucket,
		Object:     obj,
		Host:       "Internal: [ILM-EXPIRY]",
	})
	return 0
}

// objectPath returns the prefix and object name.
func (i *crawlItem) objectPath() string {
	return path.Join(i.prefix, i.objectName)
}

// sleepDuration multiplies the duration d by x and sleeps if is more than 100 micro seconds.
// sleep is limited to max 1 second.
func sleepDuration(d time.Duration, x float64) {
	// Don't sleep for really small amount of time
	if d := time.Duration(float64(d) * x); d > time.Microsecond*100 {
		if d > time.Second {
			d = time.Second
		}
		time.Sleep(d)
	}
}

// healReplication will heal a scanned item that has failed replication.
func (i *crawlItem) healReplication(ctx context.Context, o ObjectLayer, meta actionMeta) {
	if meta.oi.ReplicationStatus == replication.Pending ||
		meta.oi.ReplicationStatus == replication.Failed {
		// if heal encounters a pending replication status, either replication
		// has failed due to server shutdown or crawler and PutObject replication are in contention.
		healPending := meta.oi.ReplicationStatus == replication.Pending
		replicateObject(ctx, meta.oi.Bucket, meta.oi.Name, meta.oi.VersionID, o, nil, healPending)
	}
}
