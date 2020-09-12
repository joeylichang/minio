/*
 * MinIO Cloud Storage, (C) 2016-2020 MinIO, Inc.
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
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/minio/minio/cmd/logger"
	"github.com/minio/minio/pkg/bpool"
	"github.com/minio/minio/pkg/color"
	"github.com/minio/minio/pkg/dsync"
	"github.com/minio/minio/pkg/madmin"
	"github.com/minio/minio/pkg/sync/errgroup"
)

// OfflineDisk represents an unavailable disk.
var OfflineDisk StorageAPI // zero value is nil

// partialOperation is a successful upload/delete of an object
// but not written in all disks (having quorum)
/*
 * 写、删除 部分 EC 分块数据，但是达到了 quorme的要求了
 */
type partialOperation struct {
	bucket    string
	object    string
	versionID string
	failedSet int
}

// erasureObjects - Implements ER object layer.
type erasureObjects struct {
	GatewayUnsupported

	/*
	 * 函数的回调，全部来自 erasureZone，内部通过 disk、locker、endpoints 数据 根据索引（内部加锁）获取对应的资源
	 *
	 * nsMutex、bp、mrfOpCh 也在 erasureZone 初始化的时候初始化了
	 */

	// getDisks returns list of storageAPIs.
	getDisks func() []StorageAPI

	// getLockers returns list of remote and local lockers.
	getLockers func() []dsync.NetLocker

	// getEndpoints returns list of endpoint strings belonging this set.
	// some may be local and some remote.
	getEndpoints func() []string

	// Locker mutex map.
	/*
	 * 针对 volume 绝对路径（资源）的读写锁
	 */
	nsMutex *nsLockMap 	

	// Byte pools used for temporary i/o buffers.
	bp *bpool.BytePoolCap

	mrfOpCh chan partialOperation
}

// NewNSLock - initialize a new namespace RWLocker instance.
/*
 * 设计：
 * 	1. 在 ErasureSets 中，nsMutex 会被初始化 nsLock 对象，但是这并不是最终的使用对象（类似一种工厂设计模式，后面会根据实际情况，使用不同类型的锁）。
 * 	2. 如果是 isDistErasure ，会在 er.nsMutex.NewNSLock 内部生成 distLockInstance 类型的锁（否则是 localLockInstance ，在这里我们关注 distLockInstance）。
 * 	3. er.getLockers 是一个接口，distLockInstance GetLock、Unlock 等成员函数最终调用的正是这个接口的实现，所以要理解锁的逻辑，要深入理解 distLockInstance 和 er.getLockers 具体实现的逻辑。
 * 
 * 实现：
 * distLockInstance 部分实现
 * 		type distLockInstance struct {
 *			rwMutex *dsync.DRWMutex 	// minio/pkg 提供的分布式锁
 *			opsID   string 				// 锁的全局 ID 随机生成
 *			ctx     context.Context 	
 *		}
 *
 * 		GetLock、GetRLock : 获取资源的锁、或者读锁，内部会加一个参数给远程的接口，既锁源
 * 			对应的锁源，既调用位置（文件名、行号、函数名），既 [filename:lineNum:funcName()]
 * 		Unlock、RUnlock 	: 释放对应资源的锁或者读锁
 * 
 * localLocker 部分实现
 * 		er.getLockers = newLockAPI 
 * 			1. 本地 globalLockServers[endpoint]（*localLocker类型）
 * 			2. 远程 newlockRESTClient，在远程也是 globalLockServers[endpoint] 的类型，所以逻辑基本一致
 * 			3. 一个 disk 维护一个 localLocker，下面看一下 localLocker 内部逻辑（相对简单）
 * 
 * 			type localLocker struct {
 *				mutex    sync.Mutex 	// 加锁、释放锁 修改 map 等资源的局部锁
 *				endpoint Endpoint 		// 每一个 disk 会给所有的 disks 创建一个 globalLockServers 接口，既单独视角内有全局的分布式锁，代表某一个 disk 上面的锁
 * 				lockMap  map[string][]lockRequesterInfo
 * 				// map 的 key 是锁定的资源，可以一次锁定多个资源，如果是多个资源必须保证全部可以上锁才会上锁（写锁的话要看是否全部资源没被抢占）
 * 				// 
 * 			}
 * 	
 * 		type lockRequesterInfo struct {
 *			Writer        bool      // 是否是写锁
 *			UID           string    // 客户端生成的随机 ID，在 distLockInstance 内部生成
 *			Timestamp     time.Time // 锁初始化的时间
 *			TimeLastCheck time.Time // 最后一次 check 的时间，分布式锁方式远程节点异常导致无法解锁，需要设置超时时间
 *			Source        string    // 锁定的资源，既 distLockInstance 中的 [filename:lineNum:funcName()]
 * 		}
 *
 * 		在分布式锁的 http 服务接口模块内会单独启动一个协程，对锁进行周期检查
 * 		0. 分钟级的随机时间，进行check
 * 		1. 获取当前节点上所有过期的锁（超过2min 没有被check）
 * 		2. 向 zone0 中所有的节点发送 expire 请求，判断节点是否过期
 * 			2.1 对于网络异常、当前节点判断其他节点不在线，等情况都视为成功
 * 		3. 如果判断没有对应资源的锁的节点个数少于 quorum，则删除 locakname 下对应的 lockinfo
 * 			3.1 读锁 quorum = globalErasureSetDriveCount（EC编码分块数量） / 2
 * 			3.2 写锁 quorum = globalErasureSetDriveCount（EC编码分块数量） / 2 + 1
 * 		注意： 
 *			1. 为什么不是机器数量，而是分块数量呢？很奇怪
 * 				如果是 64台机器，分块数量肯定是 32
 * 				可以理解了：因为锁也是分配到 Set 内部的对象资源，在 zone 中会进行 hash 选择 set
 * 			2. 所有的锁都在 zone0，所以只需要向 zone0 发送请求即可	
 */
func (er erasureObjects) NewNSLock(ctx context.Context, bucket string, objects ...string) RWLocker {
	return er.nsMutex.NewNSLock(ctx, er.getLockers, bucket, objects...)
}

// Shutdown function for object storage interface.
/*
 * xlstorage 内部的close 全部返回 nil，相当于接口未实现
 */
func (er erasureObjects) Shutdown(ctx context.Context) error {
	// Add any object layer shutdown activities here.
	closeStorageDisks(er.getDisks())
	return nil
}

// byDiskTotal is a collection satisfying sort.Interface.
/*
 * 按总空间排序
 */
type byDiskTotal []madmin.Disk

func (d byDiskTotal) Len() int      { return len(d) }
func (d byDiskTotal) Swap(i, j int) { d[i], d[j] = d[j], d[i] }
func (d byDiskTotal) Less(i, j int) bool {
	return d[i].TotalSpace < d[j].TotalSpace
}

func diskErrToDriveState(err error) (state string) {
	state = madmin.DriveStateUnknown
	switch err {
	case errDiskNotFound:
		state = madmin.DriveStateOffline
	case errCorruptedFormat:
		state = madmin.DriveStateCorrupt
	case errUnformattedDisk:
		state = madmin.DriveStateUnformatted
	case errDiskAccessDenied:
		state = madmin.DriveStatePermission
	case errFaultyDisk:
		state = madmin.DriveStateFaulty
	case nil:
		state = madmin.DriveStateOk
	}
	return
}

// getDisksInfo - fetch disks info across all other storage API.
func getDisksInfo(disks []StorageAPI, endpoints []string) (disksInfo []madmin.Disk, errs []error, onlineDisks, offlineDisks madmin.BackendDisks) {
	disksInfo = make([]madmin.Disk, len(disks))
	onlineDisks = make(madmin.BackendDisks)
	offlineDisks = make(madmin.BackendDisks)

	for _, ep := range endpoints {
		if _, ok := offlineDisks[ep]; !ok {
			offlineDisks[ep] = 0
		}
		if _, ok := onlineDisks[ep]; !ok {
			onlineDisks[ep] = 0
		}
	}

	g := errgroup.WithNErrs(len(disks))
	for index := range disks {
		index := index
		g.Go(func() error {
			if disks[index] == OfflineDisk {
				disksInfo[index] = madmin.Disk{
					State:    diskErrToDriveState(errDiskNotFound),
					Endpoint: endpoints[index],
				}
				// Storage disk is empty, perhaps ignored disk or not available.
				return errDiskNotFound
			}
			info, err := disks[index].DiskInfo()
			if err != nil {
				if !IsErr(err, baseErrs...) {
					reqInfo := (&logger.ReqInfo{}).AppendTags("disk", disks[index].String())
					ctx := logger.SetReqInfo(GlobalContext, reqInfo)
					logger.LogIf(ctx, err)
				}
			}
			di := madmin.Disk{
				Endpoint:       endpoints[index], 	// host:dir
				DrivePath:      info.MountPath, 	// 挂在路径
				TotalSpace:     info.Total,  		// 总空间
				UsedSpace:      info.Used, 			// 使用空间
				AvailableSpace: info.Free, 			// 剩余空间
				UUID:           info.ID, 			// 也就是 format.This 
				State:          diskErrToDriveState(err), // 错误信息
			}
			if info.Total > 0 {
				di.Utilization = float64(info.Used / info.Total * 100)
			}
			disksInfo[index] = di
			return err
		}, index)
	}

	errs = g.Wait()
	// Wait for the routines.
	for i, diskInfoErr := range errs {
		ep := endpoints[i]
		if diskInfoErr != nil {
			offlineDisks[ep]++
			continue
		}
		onlineDisks[ep]++
	}

	// Success.
	return disksInfo, errs, onlineDisks, offlineDisks
}

// Get an aggregated storage info across all disks.
func getStorageInfo(disks []StorageAPI, endpoints []string) (StorageInfo, []error) {
	disksInfo, errs, onlineDisks, offlineDisks := getDisksInfo(disks, endpoints)

	// Sort so that the first element is the smallest.
	sort.Sort(byDiskTotal(disksInfo))

	storageInfo := StorageInfo{
		Disks: disksInfo,
	}

	storageInfo.Backend.Type = BackendErasure 		// 类型，EC、distEC 都是 BackendErasure
	storageInfo.Backend.OnlineDisks = onlineDisks 	// 请求成功返回的磁盘信息，endpoints -> num
	storageInfo.Backend.OfflineDisks = offlineDisks // 请求未成功返回的磁盘信息，endpoints -> num

	return storageInfo, errs
}

// StorageInfo - returns underlying storage statistics.
func (er erasureObjects) StorageInfo(ctx context.Context, local bool) (StorageInfo, []error) {
	disks := er.getDisks()
	endpoints := er.getEndpoints()
	if local {
		var localDisks []StorageAPI
		var localEndpoints []string
		for i, disk := range disks {
			if disk != nil {
				if disk.IsLocal() {
					// Append this local disk since local flag is true
					localDisks = append(localDisks, disk)
					localEndpoints = append(localEndpoints, endpoints[i])
				}
			}
		}
		disks = localDisks
		endpoints = localEndpoints
	}
	return getStorageInfo(disks, endpoints)
}

// GetMetrics - is not implemented and shouldn't be called.
func (er erasureObjects) GetMetrics(ctx context.Context) (*Metrics, error) {
	logger.LogIf(ctx, NotImplemented{})
	return &Metrics{}, NotImplemented{}
}

// CrawlAndGetDataUsage collects usage from all buckets.
// updates are sent as different parts of the underlying
// structure has been traversed.
func (er erasureObjects) CrawlAndGetDataUsage(ctx context.Context, bf *bloomFilter, updates chan<- DataUsageInfo) error {
	return NotImplemented{API: "CrawlAndGetDataUsage"}
}

// CrawlAndGetDataUsage will start crawling buckets and send updated totals as they are traversed.
// Updates are sent on a regular basis and the caller *must* consume them.
func (er erasureObjects) crawlAndGetDataUsage(ctx context.Context, buckets []BucketInfo, bf *bloomFilter, updates chan<- dataUsageCache) error {
	var disks []StorageAPI

	for _, d := range er.getLoadBalancedDisks() {
		if d == nil || !d.IsOnline() {
			continue
		}
		disks = append(disks, d)
	}
	if len(disks) == 0 || len(buckets) == 0 {
		return nil
	}

	// Load bucket totals
	oldCache := dataUsageCache{}
	/*
	 * oldCache 加载 下面路径的数据（GetObject）
	 * /diskpath/.minio.sys/buckets/.usage-cache.bin
	 */
	err := oldCache.load(ctx, er, dataUsageCacheName)
	if err != nil {
		return err
	}

	// New cache..
	cache := dataUsageCache{
		Info: dataUsageCacheInfo{
			Name:      dataUsageRoot, // "/"
			NextCycle: oldCache.Info.NextCycle,
		},
		Cache: make(map[string]dataUsageEntry, len(oldCache.Cache)),
	}
	bloom := bf.bytes()

	// Put all buckets into channel.
	bucketCh := make(chan BucketInfo, len(buckets))
	// Add new buckets first
	for _, b := range buckets {
		if oldCache.find(b.Name) == nil {
			bucketCh <- b
		}
	}

	// Add existing buckets if changes or lifecycles.
	for _, b := range buckets {
		e := oldCache.find(b.Name)
		if e != nil {
			cache.replace(b.Name, dataUsageRoot, *e)
			lc, err := globalLifecycleSys.Get(b.Name)
			activeLC := err == nil && lc.HasActiveRules("", true)
			if activeLC || bf == nil || bf.containsDir(b.Name) {
				bucketCh <- b
			} else {
				if intDataUpdateTracker.debug {
					logger.Info(color.Green("crawlAndGetDataUsage:")+" Skipping bucket %v, not updated", b.Name)
				}
			}
		}
	}

	close(bucketCh)
	bucketResults := make(chan dataUsageEntryInfo, len(disks))

	// Start async collector/saver.
	// This goroutine owns the cache.
	var saverWg sync.WaitGroup
	saverWg.Add(1)
	go func() {
		const updateTime = 30 * time.Second
		t := time.NewTicker(updateTime)
		defer t.Stop()
		defer saverWg.Done()
		var lastSave time.Time

	saveLoop:
		for {
			select {
			case <-ctx.Done():
				// Return without saving.
				return
			case <-t.C:
				if cache.Info.LastUpdate.Equal(lastSave) {
					continue
				}
				logger.LogIf(ctx, cache.save(ctx, er, dataUsageCacheName))
				updates <- cache.clone()
				lastSave = cache.Info.LastUpdate
			case v, ok := <-bucketResults:
				if !ok {
					break saveLoop
				}
				cache.replace(v.Name, v.Parent, v.Entry)
				cache.Info.LastUpdate = time.Now()
			}
		}
		// Save final state...
		cache.Info.NextCycle++
		cache.Info.LastUpdate = time.Now()
		logger.LogIf(ctx, cache.save(ctx, er, dataUsageCacheName))
		updates <- cache
	}()

	// Start one crawler per disk
	var wg sync.WaitGroup
	wg.Add(len(disks))
	for i := range disks {
		go func(i int) {
			defer wg.Done()
			disk := disks[i]

			for bucket := range bucketCh {
				select {
				case <-ctx.Done():
					return
				default:
				}

				// Load cache for bucket
				/*
				 * ${Buckets}/.usage-cache.bin
				 */
				cacheName := pathJoin(bucket.Name, dataUsageCacheName)
				cache := dataUsageCache{}
				logger.LogIf(ctx, cache.load(ctx, er, cacheName))
				if cache.Info.Name == "" {
					cache.Info.Name = bucket.Name
				}
				cache.Info.BloomFilter = bloom
				if cache.Info.Name != bucket.Name {
					logger.LogIf(ctx, fmt.Errorf("cache name mismatch: %s != %s", cache.Info.Name, bucket.Name))
					cache.Info = dataUsageCacheInfo{
						Name:       bucket.Name,
						LastUpdate: time.Time{},
						NextCycle:  0,
					}
				}

				// Calc usage
				before := cache.Info.LastUpdate
				cache, err = disk.CrawlAndGetDataUsage(ctx, cache)
				cache.Info.BloomFilter = nil
				if err != nil {
					logger.LogIf(ctx, err)
					if cache.Info.LastUpdate.After(before) {
						logger.LogIf(ctx, cache.save(ctx, er, cacheName))
					}
					continue
				}

				var root dataUsageEntry
				if r := cache.root(); r != nil {
					root = cache.flatten(*r)
				}
				bucketResults <- dataUsageEntryInfo{
					Name:   cache.Info.Name,
					Parent: dataUsageRoot,
					Entry:  root,
				}
				// Save cache
				logger.LogIf(ctx, cache.save(ctx, er, cacheName))
			}
		}(i)
	}
	wg.Wait()
	close(bucketResults)
	saverWg.Wait()

	return nil
}

// Health shouldn't be called directly - will panic
func (er erasureObjects) Health(ctx context.Context, _ HealthOptions) HealthResult {
	logger.CriticalIf(ctx, NotImplemented{})
	return HealthResult{}
}
