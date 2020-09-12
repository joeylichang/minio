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
	"sort"

	"github.com/minio/minio-go/v7/pkg/s3utils"
	"github.com/minio/minio/cmd/logger"
	"github.com/minio/minio/pkg/sync/errgroup"
)

// list all errors that can be ignore in a bucket operation.
var bucketOpIgnoredErrs = append(baseIgnoredErrs, errDiskAccessDenied, errUnformattedDisk)

// list all errors that can be ignored in a bucket metadata operation.
var bucketMetadataOpIgnoredErrs = append(bucketOpIgnoredErrs, errVolumeNotFound)

/// Bucket operations

/*
 * Bucket 级别的操作
 * 	MakeBucket
 * 	GetBucketInfo（单个信息）
 * 	ListBuckets（全部信息）
 * 	DeleteBucket
 */


// MakeBucket - make a bucket.
/*
 * 获取 erasureObjects 的 disks，调用本地、远程disk 的 makevol 接口创建路径
 * 创建 diskpath/volume 文件，所以 volume 就是 bucket 
 */
func (er erasureObjects) MakeBucketWithLocation(ctx context.Context, bucket string, opts BucketOptions) error {
	// Verify if bucket is valid.
	if err := s3utils.CheckValidBucketNameStrict(bucket); err != nil {
		return BucketNameInvalid{Bucket: bucket}
	}

	storageDisks := er.getDisks()

	g := errgroup.WithNErrs(len(storageDisks))

	// Make a volume entry on all underlying storage disks.
	for index := range storageDisks {
		index := index
		g.Go(func() error {
			if storageDisks[index] != nil {
				if err := storageDisks[index].MakeVol(bucket); err != nil {
					if err != errVolumeExists {
						logger.LogIf(ctx, err)
					}
					return err
				}
				return nil
			}
			return errDiskNotFound
		}, index)
	}

	writeQuorum := getWriteQuorum(len(storageDisks))
	err := reduceWriteQuorumErrs(ctx, g.Wait(), bucketOpIgnoredErrs, writeQuorum)
	return toObjectErr(err, bucket)
}

func undoDeleteBucket(storageDisks []StorageAPI, bucket string) {
	g := errgroup.WithNErrs(len(storageDisks))
	// Undo previous make bucket entry on all underlying storage disks.
	for index := range storageDisks {
		if storageDisks[index] == nil {
			continue
		}
		index := index
		g.Go(func() error {
			_ = storageDisks[index].MakeVol(bucket)
			return nil
		}, index)
	}

	// Wait for all make vol to finish.
	g.Wait()
}

// getBucketInfo - returns the BucketInfo from one of the load balanced disks.
/*
 * 在所有的磁盘中重拍顺序但是相对顺序不变，然后按顺序发送 StatVol 请求，有一个返回则成功则直接返回
 * 返回的信息 BucketInfo 包括volume 名字 和 最后的修改时间
 */
func (er erasureObjects) getBucketInfo(ctx context.Context, bucketName string) (bucketInfo BucketInfo, err error) {
	var bucketErrs []error
	/*
	 * getLoadBalancedDisks 重新对 Disk 随机排序，但是排序的相对位置不变
	 * 1，2，3，4，5，6
	 * 3，4，5，6，1，2
	 */
	for _, disk := range er.getLoadBalancedDisks() {
		if disk == nil {
			bucketErrs = append(bucketErrs, errDiskNotFound)
			continue
		}
		/*
		 * disk.StatVol 返回 volume 名字，以及最后修改时间（作为创建时间）
		 */
		volInfo, serr := disk.StatVol(bucketName)
		if serr == nil {
			return BucketInfo(volInfo), nil
		}
		err = serr
		// For any reason disk went offline continue and pick the next one.
		if IsErrIgnored(err, bucketMetadataOpIgnoredErrs...) {
			bucketErrs = append(bucketErrs, err)
			continue
		}
		// Any error which cannot be ignored, we return quickly.
		return BucketInfo{}, err
	}
	// If all our errors were ignored, then we try to
	// reduce to one error based on read quorum.
	// `nil` is deliberately passed for ignoredErrs
	// because these errors were already ignored.
	readQuorum := getReadQuorum(len(er.getDisks()))
	return BucketInfo{}, reduceReadQuorumErrs(ctx, bucketErrs, nil, readQuorum)
}

// GetBucketInfo - returns BucketInfo for a bucket.
func (er erasureObjects) GetBucketInfo(ctx context.Context, bucket string) (bi BucketInfo, e error) {
	bucketInfo, err := er.getBucketInfo(ctx, bucket)
	if err != nil {
		return bi, toObjectErr(err, bucket)
	}
	return bucketInfo, nil
}

// listBuckets - returns list of all buckets from a disk picked at random.
func (er erasureObjects) listBuckets(ctx context.Context) (bucketsInfo []BucketInfo, err error) {
	for _, disk := range er.getLoadBalancedDisks() {
		if disk == nil {
			continue
		}
		var volsInfo []VolInfo
		volsInfo, err = disk.ListVols()
		if err == nil {
			// NOTE: The assumption here is that volumes across all disks in
			// readQuorum have consistent view i.e they all have same number
			// of buckets. This is essentially not verified since healing
			// should take care of this.
			var bucketsInfo []BucketInfo
			for _, volInfo := range volsInfo {
				/*
				 * s3 命名规范
				 */
				if isReservedOrInvalidBucket(volInfo.Name, true) {
					continue
				}
				bucketsInfo = append(bucketsInfo, BucketInfo(volInfo))
			}
			// For buckets info empty, loop once again to check
			// if we have, can happen if disks were down.
			if len(bucketsInfo) == 0 {
				continue
			}
			return bucketsInfo, nil
		}
		logger.LogIf(ctx, err)
		// Ignore any disks not found.
		if IsErrIgnored(err, bucketMetadataOpIgnoredErrs...) {
			continue
		}
		break
	}
	return nil, err
}

// ListBuckets - lists all the buckets, sorted by its name.
func (er erasureObjects) ListBuckets(ctx context.Context) ([]BucketInfo, error) {
	bucketInfos, err := er.listBuckets(ctx)
	if err != nil {
		return nil, toObjectErr(err)
	}
	// Sort by bucket name before returning.
	sort.Sort(byBucketName(bucketInfos))
	return bucketInfos, nil
}

// Dangling buckets should be handled appropriately, in this following situation
// we actually have quorum error to be `nil` but we have some disks where
// the bucket delete returned `errVolumeNotEmpty` but this is not correct
// can only happen if there are dangling objects in a bucket. Under such
// a situation we simply attempt a full delete of the bucket including
// the dangling objects. All of this happens under a lock and there
// is no way a user can create buckets and sneak in objects into namespace,
// so it is safer to do.
/*
 * 针对 errVolumeNotEmpty 错误的磁盘进行前置删除
 */
func deleteDanglingBucket(ctx context.Context, storageDisks []StorageAPI, dErrs []error, bucket string) {
	for index, err := range dErrs {
		if err == errVolumeNotEmpty {
			// Attempt to delete bucket again.
			if derr := storageDisks[index].DeleteVol(bucket, false); derr == errVolumeNotEmpty {
				_ = cleanupDir(ctx, storageDisks[index], bucket, "")

				_ = storageDisks[index].DeleteVol(bucket, false)

				// Cleanup all the previously incomplete multiparts.
				_ = cleanupDir(ctx, storageDisks[index], minioMetaMultipartBucket, bucket)
			}
		}
	}
}

// DeleteBucket - deletes a bucket.
/*
 * 1. 首先，删除，/diskpath/bufket
 * 2. 其次，删除元数据，/diskpath/.minio.sys/multipart/bucket 下面的内容
 * 3. 最后处理异常情况：
 * 	3.1 如果设置了 forceDelete，则有一个 disk 返回 err，则恢复所有的 bucket
 * 	3.2 如果没有达到 qurom ，则恢复所有的 bucket
 * 	3.3 针对 errVolumeNotEmpty 错误进行处理（悬空桶），只有有悬空对象才会有这种问题（既删除的一定是空桶），此时会强制删除
 * 	注意：针对 3.3 注释说应该在锁的情况下进行更安全（可能是在上层调用）。
 */
func (er erasureObjects) DeleteBucket(ctx context.Context, bucket string, forceDelete bool) error {
	// Collect if all disks report volume not found.
	storageDisks := er.getDisks()

	g := errgroup.WithNErrs(len(storageDisks))

	for index := range storageDisks {
		index := index
		g.Go(func() error {
			if storageDisks[index] != nil {
				/*
				 * delete dir /diskpath/volume
				 */
				if err := storageDisks[index].DeleteVol(bucket, forceDelete); err != nil {
					return err
				}
				/*
				 * minioMetaMultipartBucket = .minio.sys/multipart
				 * 1. 递归 delete /diskpath/.minio.sys/multipart/bucket
				 */
				err := cleanupDir(ctx, storageDisks[index], minioMetaMultipartBucket, bucket)
				if err != nil && err != errVolumeNotFound {
					return err
				}
				return nil
			}
			return errDiskNotFound
		}, index)
	}

	// Wait for all the delete vols to finish.
	dErrs := g.Wait()

	if forceDelete {
		for _, err := range dErrs {
			if err != nil {
				/*
				 * 有一个磁盘没有删除成功，则全部执行 makevol 重新创建 bucket
				 */
				undoDeleteBucket(storageDisks, bucket)
				return toObjectErr(err, bucket)
			}
		}

		return nil
	}

	writeQuorum := getWriteQuorum(len(storageDisks))
	err := reduceWriteQuorumErrs(ctx, dErrs, bucketOpIgnoredErrs, writeQuorum)
	if err == errErasureWriteQuorum {
		undoDeleteBucket(storageDisks, bucket)
	}
	if err != nil {
		return toObjectErr(err, bucket)
	}

	// If we reduce quorum to nil, means we have deleted buckets properly
	// on some servers in quorum, we should look for volumeNotEmpty errors
	// and delete those buckets as well.
	deleteDanglingBucket(ctx, storageDisks, dErrs, bucket)

	return nil
}

// IsNotificationSupported returns whether bucket notification is applicable for this layer.
func (er erasureObjects) IsNotificationSupported() bool {
	return true
}

// IsListenSupported returns whether listen bucket notification is applicable for this layer.
func (er erasureObjects) IsListenSupported() bool {
	return true
}

// IsEncryptionSupported returns whether server side encryption is implemented for this layer.
func (er erasureObjects) IsEncryptionSupported() bool {
	return true
}

// IsCompressionSupported returns whether compression is applicable for this layer.
func (er erasureObjects) IsCompressionSupported() bool {
	return true
}

// IsTaggingSupported indicates whether erasureObjects implements tagging support.
func (er erasureObjects) IsTaggingSupported() bool {
	return true
}
