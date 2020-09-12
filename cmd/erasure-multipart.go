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
	"io"
	"path"
	"sort"
	"strconv"
	"strings"

	"github.com/minio/minio-go/v7/pkg/set"
	xhttp "github.com/minio/minio/cmd/http"
	"github.com/minio/minio/cmd/logger"
	"github.com/minio/minio/pkg/mimedb"
	"github.com/minio/minio/pkg/sync/errgroup"
)

func (er erasureObjects) getUploadIDDir(bucket, object, uploadID string) string {
	return pathJoin(er.getMultipartSHADir(bucket, object), uploadID)
}

func (er erasureObjects) getMultipartSHADir(bucket, object string) string {
	return getSHA256Hash([]byte(pathJoin(bucket, object)))
}

// checkUploadIDExists - verify if a given uploadID exists and is valid.
func (er erasureObjects) checkUploadIDExists(ctx context.Context, bucket, object, uploadID string) error {
	_, _, _, err := er.getObjectFileInfo(ctx, minioMetaMultipartBucket, er.getUploadIDDir(bucket, object, uploadID), ObjectOptions{})
	return err
}

// Removes part given by partName belonging to a mulitpart upload from minioMetaBucket
func (er erasureObjects) removeObjectPart(bucket, object, uploadID, dataDir string, partNumber int) {
	uploadIDPath := er.getUploadIDDir(bucket, object, uploadID)
	curpartPath := pathJoin(uploadIDPath, dataDir, fmt.Sprintf("part.%d", partNumber))
	storageDisks := er.getDisks()

	g := errgroup.WithNErrs(len(storageDisks))
	for index, disk := range storageDisks {
		if disk == nil {
			continue
		}
		index := index
		g.Go(func() error {
			// Ignoring failure to remove parts that weren't present in CompleteMultipartUpload
			// requests. xl.meta is the authoritative source of truth on which parts constitute
			// the object. The presence of parts that don't belong in the object doesn't affect correctness.
			_ = storageDisks[index].DeleteFile(minioMetaMultipartBucket, curpartPath)
			return nil
		}, index)
	}
	g.Wait()
}

// ListMultipartUploads - lists all the pending multipart
// uploads for a particular object in a bucket.
//
// Implements minimal S3 compatible ListMultipartUploads API. We do
// not support prefix based listing, this is a deliberate attempt
// towards simplification of multipart APIs.
// The resulting ListMultipartsInfo structure is unmarshalled directly as XML.
/* 
 * scan 指定 bucket、object 下面所有的 Multi 对象信息，uploadIDMarker 是标记
 * 返回的 result 中有下一次的标记
 */
func (er erasureObjects) ListMultipartUploads(ctx context.Context, bucket, object, keyMarker, uploadIDMarker, delimiter string, maxUploads int) (result ListMultipartsInfo, err error) {
	result.MaxUploads = maxUploads
	result.KeyMarker = keyMarker
	result.Prefix = object
	result.Delimiter = delimiter

	var uploadIDs []string
	for _, disk := range er.getLoadBalancedDisks() {
		if disk == nil {
			continue
		}
		uploadIDs, err = disk.ListDir(minioMetaMultipartBucket, er.getMultipartSHADir(bucket, object), -1)
		if err != nil {
			if err == errDiskNotFound {
				continue
			}
			if err == errFileNotFound {
				return result, nil
			}
			logger.LogIf(ctx, err)
			return result, toObjectErr(err, bucket, object)
		}
		break
	}

	for i := range uploadIDs {
		uploadIDs[i] = strings.TrimSuffix(uploadIDs[i], SlashSeparator)
	}

	// S3 spec says uploadIDs should be sorted based on initiated time, we need
	// to read the metadata entry.
	var uploads []MultipartInfo

	populatedUploadIds := set.NewStringSet()

retry:
	for _, disk := range er.getLoadBalancedDisks() {
		if disk == nil {
			continue
		}
		for _, uploadID := range uploadIDs {
			if populatedUploadIds.Contains(uploadID) {
				continue
			}
			fi, err := disk.ReadVersion(minioMetaMultipartBucket, pathJoin(er.getUploadIDDir(bucket, object, uploadID)), "")
			if err != nil {
				if err == errDiskNotFound || err == errFileNotFound {
					goto retry
				}
				return result, toObjectErr(err, bucket, object)
			}
			populatedUploadIds.Add(uploadID)
			uploads = append(uploads, MultipartInfo{
				Object:    object,
				UploadID:  uploadID,
				Initiated: fi.ModTime,
			})
		}
		break
	}

	sort.Slice(uploads, func(i int, j int) bool {
		return uploads[i].Initiated.Before(uploads[j].Initiated)
	})

	uploadIndex := 0
	if uploadIDMarker != "" {
		for uploadIndex < len(uploads) {
			if uploads[uploadIndex].UploadID != uploadIDMarker {
				uploadIndex++
				continue
			}
			if uploads[uploadIndex].UploadID == uploadIDMarker {
				uploadIndex++
				break
			}
			uploadIndex++
		}
	}
	for uploadIndex < len(uploads) {
		result.Uploads = append(result.Uploads, uploads[uploadIndex])
		result.NextUploadIDMarker = uploads[uploadIndex].UploadID
		uploadIndex++
		if len(result.Uploads) == maxUploads {
			break
		}
	}

	result.IsTruncated = uploadIndex < len(uploads)

	if !result.IsTruncated {
		result.NextKeyMarker = ""
		result.NextUploadIDMarker = ""
	}

	return result, nil
}

// newMultipartUpload - wrapper for initializing a new multipart
// request; returns a unique upload id.
//
// Internally this function creates 'uploads.json' associated for the
// incoming object at
// '.minio.sys/multipart/bucket/object/uploads.json' on all the
// disks. `uploads.json` carries metadata regarding on-going multipart
// operation(s) on the object.
/*
 * type FileInfo struct {
 *		Volume string
 *		Name string
 *		VersionID string 		// 用户指定的 version，没有则是随机值
 *		IsLatest bool
 *		Deleted bool
 *		DataDir string 			// 随机值
 *		XLV1 bool
 *		ModTime time.Time 		
 *		Size int64
 *		Mode os.FileMode
 *		Metadata map[string]string 	// 用户指定的一组元数据
 *		Parts []ObjectPartInfo
 *		Erasure ErasureInfo
 * 			type ErasureInfo struct {
 *				Algorithm string  	// RS 校验码算法名 "rs-vandermonde"
 *				DataBlocks int  	// 数据块个数
 *				ParityBlocks int  	// 校验块个数
 *				BlockSize int64  	// 10 Mib = 10 * 1024 * 1024 数据的大小，而不是分块的大小，需要除以 datablocks
 *				Index int  			// 分块的索引，从 1 开始计算
 *				Distribution []int  // block 分布顺序，345612
 *				Checksums []ChecksumInfo 
 *			}
 * }
 * 
 * 逻辑：根据参数初始化 FileInfo 对象，写入所有磁盘 /diskpath/.minio.sys/multipart/sha256hash(bucket/object)/uploadID/xl.meta 内
 * 既 FileInfo 落盘之后是 xl.meta
 */
func (er erasureObjects) newMultipartUpload(ctx context.Context, bucket string, object string, opts ObjectOptions) (string, error) {

	onlineDisks := er.getDisks()
	/*
	 * 根据冗余度配置，决定校验块个数，默认是 总块数的一半，低冗余度 是2块
	 */
	parityBlocks := globalStorageClass.GetParityForSC(opts.UserDefined[xhttp.AmzStorageClass])
	if parityBlocks == 0 {
		parityBlocks = len(onlineDisks) / 2
	}
	dataBlocks := len(onlineDisks) - parityBlocks

	/*
	 * 内部完成 FileInfo.ErasureInfo 的初始化
	 */
	fi := newFileInfo(object, dataBlocks, parityBlocks)  

	// we now know the number of blocks this object needs for data and parity.
	// establish the writeQuorum using this data
	writeQuorum := dataBlocks
	if dataBlocks == parityBlocks {
		writeQuorum = dataBlocks + 1
	}

	if opts.UserDefined["content-type"] == "" {
		contentType := mimedb.TypeByExtension(path.Ext(object))
		opts.UserDefined["content-type"] = contentType
	}

	// Calculate the version to be saved.
	if opts.Versioned {
		fi.VersionID = opts.VersionID
		if fi.VersionID == "" {
			fi.VersionID = mustGetUUID()
		}
	}

	fi.DataDir = mustGetUUID()
	fi.ModTime = UTCNow()
	if opts.UserDefined != nil {
		fi.Metadata = opts.UserDefined
	} else {
		fi.Metadata = make(map[string]string)
	}

	uploadID := mustGetUUID()
	/*
	 * uploadIDPath = sha256hash(bucket/object)/uploadID
	 * 应该是两端随机值
	 */
	uploadIDPath := er.getUploadIDDir(bucket, object, uploadID)
	tempUploadIDPath := uploadID

	// Delete the tmp path later in case we fail to commit (ignore
	// returned errors) - this will be a no-op in case of a commit
	// success.
	/*
	 * 删除 minioMetaTmpBucket/tempUploadIDPath 目录及其子目录
	 * minioMetaTmpBucket = /diskpath/.minio.sys/tmp/ 
	 * tempUploadIDPath = uploadID
	 */
	defer er.deleteObject(ctx, minioMetaTmpBucket, tempUploadIDPath, writeQuorum)

	var partsMetadata = make([]FileInfo, len(onlineDisks))
	for i := range onlineDisks {
		partsMetadata[i] = fi
	}

	var err error
	// Write updated `xl.meta` to all disks.
	/*
	 * 将上述 fi 的内容写入所有磁盘的，/diskpath/.minio.sys/tmp/sha256hash(bucket/object)/uploadID/xl.meta
	 */
	onlineDisks, err = writeUniqueFileInfo(ctx, onlineDisks, minioMetaTmpBucket, tempUploadIDPath, partsMetadata, writeQuorum)
	if err != nil {
		return "", toObjectErr(err, minioMetaTmpBucket, tempUploadIDPath)
	}

	// Attempt to rename temp upload object to actual upload path object
	/* mv /diskpath/.minio.sys/multipart/sha256hash(bucket/object)/uploadID/xl.meta */
	_, err = rename(ctx, onlineDisks, minioMetaTmpBucket, tempUploadIDPath, minioMetaMultipartBucket, uploadIDPath, true, writeQuorum, nil)
	if err != nil {
		return "", toObjectErr(err, minioMetaMultipartBucket, uploadIDPath)
	}

	// Return success.
	return uploadID, nil
}

// NewMultipartUpload - initialize a new multipart upload, returns a
// unique id. The unique id returned here is of UUID form, for each
// subsequent request each UUID is unique.
//
// Implements S3 compatible initiate multipart API.
func (er erasureObjects) NewMultipartUpload(ctx context.Context, bucket, object string, opts ObjectOptions) (string, error) {
	// No metadata is set, allocate a new one.
	if opts.UserDefined == nil {
		opts.UserDefined = make(map[string]string)
	}
	return er.newMultipartUpload(ctx, bucket, object, opts)
}

// CopyObjectPart - reads incoming stream and internally erasure codes
// them. This call is similar to put object part operation but the source
// data is read from an existing object.
//
// Implements S3 compatible Upload Part Copy API.
/*
 * 仅仅 copy 一个 part
 */
func (er erasureObjects) CopyObjectPart(ctx context.Context, srcBucket, srcObject, dstBucket, dstObject, uploadID string, partID int, startOffset int64, length int64, srcInfo ObjectInfo, srcOpts, dstOpts ObjectOptions) (pi PartInfo, e error) {
	partInfo, err := er.PutObjectPart(ctx, dstBucket, dstObject, uploadID, partID, NewPutObjReader(srcInfo.Reader, nil, nil), dstOpts)
	if err != nil {
		return pi, toObjectErr(err, dstBucket, dstObject)
	}

	// Success.
	return partInfo, nil
}

// PutObjectPart - reads incoming stream and internally erasure codes
// them. This call is similar to single put operation but it is part
// of the multipart transaction.
//
// Implements S3 compatible Upload Part API.
/*
 * 参数：
 * 	bucket, object, uploadID 不用多解释了
 * 	partID
 * 	PutObjReader
 * 	ObjectOptions
 * 返回值：
 * 	PartInfo
 *
 * 作用：上传大对象，客户端或者是上层逻辑进行分片（P），按分片上传
 * 		每片如果超过10Mib，再进行分片，每个小小分片（PP）在进行 EC 编码
 * 		在组织上，多个 PP 串行写入一个文件，每个 PP 有自己的 bitrot 校验和
 * 		在 complet 会对 P 的时候会对用户时机数据进行校验，除了最后一个P，其余的都应该大于 5M
 * 1. 检查 uploadID 目录及其下文件xl.meta是否存在（在调用put 之前应该调用 newUopdateID）
 * 2. 调用 NewErasure 生成 EC 编码的信息
 * 3. 每块磁盘绑定一个 newBitrotWriter ，内部创建数据文件（直接 IO。大小等于分片数量 * EC 分块大小 + 校验和）
 * 4. 进行 EC 编码并写入数据，newBitrotWriter 内部绑定了 直接IO 的映射区域与管道，EC 编码之后向管道写入即可
 * 5. 选一个 xl.meta 进行读取（策略和前面的一样，选最新的根据 quorm选），然后修改信息，写如磁盘
 * 6. 上述写的用户数据是先写入临时文件，在进行 rename 到正式路径
 * 		"/diskpath/.minio.sys/tmp/tmpPart(ID)/part.ID"
 * 		"/diskpath/.minio.sys/multipart/sha256hash(bucket/object)/uploadID/${fi.DataDir}/part.1"
 * 		fi.DataDir 在上一步的 uoLoadID 生成的随机值
 * 		所有的 Multipart 都是放在了 .minio.sys/multipart 目录下，而不是在 /diskpath/bucket/object 下面
 * 		但是存放的路径由于 bucket/object 两个参数相关，既 sha256hash(bucket/object)
 */
func (er erasureObjects) PutObjectPart(ctx context.Context, bucket, object, uploadID string, partID int, r *PutObjReader, opts ObjectOptions) (pi PartInfo, e error) {
	data := r.Reader
	// Validate input data size and it can never be less than zero.
	if data.Size() < -1 {
		logger.LogIf(ctx, errInvalidArgument, logger.Application)
		return pi, toObjectErr(errInvalidArgument)
	}

	var partsMetadata []FileInfo
	var errs []error
	uploadIDPath := er.getUploadIDDir(bucket, object, uploadID)

	// Validates if upload ID exists.
	if err := er.checkUploadIDExists(ctx, bucket, object, uploadID); err != nil {
		return pi, toObjectErr(err, bucket, object, uploadID)
	}

	// Read metadata associated with the object from all disks.
	partsMetadata, errs = readAllFileInfo(ctx, er.getDisks(), minioMetaMultipartBucket,
		uploadIDPath, "")

	// get Quorum for this object
	_, writeQuorum, err := objectQuorumFromMeta(ctx, er, partsMetadata, errs)
	if err != nil {
		return pi, toObjectErr(err, bucket, object)
	}

	reducedErr := reduceWriteQuorumErrs(ctx, errs, objectOpIgnoredErrs, writeQuorum)
	if reducedErr == errErasureWriteQuorum {
		return pi, toObjectErr(reducedErr, bucket, object)
	}

	// List all online disks.
	onlineDisks, modTime := listOnlineDisks(er.getDisks(), partsMetadata, errs)

	// Pick one from the first valid metadata.
	fi, err := pickValidFileInfo(ctx, partsMetadata, modTime, writeQuorum)
	if err != nil {
		return pi, err
	}

	/*
	 * 根据 fi.Erasure.Distribution 顺序, 重排 onlineDisks 顺序
	 */
	onlineDisks = shuffleDisks(onlineDisks, fi.Erasure.Distribution)

	// Need a unique name for the part being written in minioMetaBucket to
	// accommodate concurrent PutObjectPart requests

	/* partID 参数指定，应该是分块的索引 */
	partSuffix := fmt.Sprintf("part.%d", partID)
	tmpPart := mustGetUUID()
	tmpPartPath := pathJoin(tmpPart, partSuffix)

	// Delete the temporary object part. If PutObjectPart succeeds there would be nothing to delete.
	/*
	 * delete /diskpath/.minio.sys/tmp/tmpPart
	 * tmpPartPath = tmpPart/part.1
	 */
	defer er.deleteObject(ctx, minioMetaTmpBucket, tmpPart, writeQuorum)

	/*
	 * type Erasure struct {
	 *		encoder                  func() reedsolomon.Encoder
	 *		dataBlocks, parityBlocks int
	 *		blockSize                int64 	// 数据的大小，而不是分块的大小，需要除以 datablocks
	 *	}
	 */
	erasure, err := NewErasure(ctx, fi.Erasure.DataBlocks, fi.Erasure.ParityBlocks, fi.Erasure.BlockSize)
	if err != nil {
		return pi, toObjectErr(err, bucket, object)
	}

	// Fetch buffer for I/O, returns from the pool if not allocates a new one and returns.
	var buffer []byte
	switch size := data.Size(); {
	case size == 0:
		buffer = make([]byte, 1) // Allocate atleast a byte to reach EOF
	case size == -1 || size >= fi.Erasure.BlockSize:
		buffer = er.bp.Get()
		defer er.bp.Put(buffer)
	case size < fi.Erasure.BlockSize:
		// No need to allocate fully fi.Erasure.BlockSize buffer if the incoming data is smaller.
		/*
		 * 容量的设置值，是怎么考虑的呢？
		 */
		buffer = make([]byte, size, 2*size+int64(fi.Erasure.ParityBlocks+fi.Erasure.DataBlocks-1))
	}

	if len(buffer) > int(fi.Erasure.BlockSize) {
		buffer = buffer[:fi.Erasure.BlockSize]
	}
	writers := make([]io.Writer, len(onlineDisks))
	for i, disk := range onlineDisks {
		if disk == nil {
			continue
		}
		/*
		 * DefaultBitrotAlgorithm = HighwayHash256S
		 * minioMetaTmpBucket = "/diskpath/.minio.sys/tmp"
		 * tmpPartPath = "uuid/part.1"
		 * erasure.ShardSize() = erasure.blockSize / erasure.dataBlocks
		 * 	数据应该是被切分了固定大小写入 part 文件，一片的大小是 blockSize（10 Mib）
		 * 	erasure.ShardSize() 表示每个分片经过 RS 编码之后的分块的大小
		 * erasure.ShardFileSize(data.Size()) = （data.Size() / e.blockSize） * erasure.ShardSize() + (data.Size() % e.blockSize) / erasure.ShardSize()
		 * 	erasure.ShardFileSize(data.Size()) 表示按 e.blockSize 分片之后每个分片的大小，既一个分块的文件大小
		 *
		 * type streamingBitrotWriter struct {
		 *		iow       *io.PipeWriter
		 *		h         hash.Hash 	// DefaultBitrotAlgorithm = HighwayHash256S
		 *		shardSize int64 		// erasure.ShardSize() 表示每个分片（最大 10Mib）经过 RS 编码之后的分块的大小
		 *		canClose  chan struct{} 
		 *	}
		 * newBitrotWriter，内部除了生成上述结构体，还在 "/diskpath/.minio.sys/tmp/uuid/part.1" 创建了直接IO的文件
		 * 最终的文件大小还包括了 HighwayHash256S 算法的校验和
		 * 内部生成一个管道 与 直接 IP 申请的映射区域绑定，在 Encode 阶段生成的数据直接写入
		 */
		writers[i] = newBitrotWriter(disk, minioMetaTmpBucket, tmpPartPath, erasure.ShardFileSize(data.Size()), DefaultBitrotAlgorithm, erasure.ShardSize())
	}

	/*
	 * 1. 内部循环将所有的数据写入（10Mib 一个分片既 Multipart 含义）
	 * 2. 内部通过 writers 的内建管道将 EC 编码和每个编码的校验和直接通过管道写入直接IO的映射区域
	 */
	n, err := erasure.Encode(ctx, data, writers, buffer, fi.Erasure.DataBlocks+1)
	closeBitrotWriters(writers)
	if err != nil {
		return pi, toObjectErr(err, bucket, object)
	}

	// Should return IncompleteBody{} error when reader has fewer bytes
	// than specified in request header.
	if n < data.Size() {
		return pi, IncompleteBody{}
	}

	for i := range writers {
		if writers[i] == nil {
			onlineDisks[i] = nil
		}
	}

	// Validates if upload ID exists.
	if err := er.checkUploadIDExists(ctx, bucket, object, uploadID); err != nil {
		return pi, toObjectErr(err, bucket, object, uploadID)
	}

	// Rename temporary part file to its final location.
	partPath := pathJoin(uploadIDPath, fi.DataDir, partSuffix)
	/*
	 *  "/diskpath/.minio.sys/tmp/tmpPart(ID)/part.1"
	 *  "/diskpath/.minio.sys/multipart/sha256hash(bucket/object)/uploadID/${fi.DataDir}/part.1"
	 */
	onlineDisks, err = rename(ctx, onlineDisks, minioMetaTmpBucket, tmpPartPath, minioMetaMultipartBucket, partPath, false, writeQuorum, nil)
	if err != nil {
		return pi, toObjectErr(err, minioMetaMultipartBucket, partPath)
	}

	// Read metadata again because it might be updated with parallel upload of another part.
	partsMetadata, errs = readAllFileInfo(ctx, onlineDisks, minioMetaMultipartBucket, uploadIDPath, "")
	reducedErr = reduceWriteQuorumErrs(ctx, errs, objectOpIgnoredErrs, writeQuorum)
	if reducedErr == errErasureWriteQuorum {
		return pi, toObjectErr(reducedErr, bucket, object)
	}

	// Get current highest version based on re-read partsMetadata.
	onlineDisks, modTime = listOnlineDisks(onlineDisks, partsMetadata, errs)

	// Pick one from the first valid metadata.
	fi, err = pickValidFileInfo(ctx, partsMetadata, modTime, writeQuorum)
	if err != nil {
		return pi, err
	}

	/*
	 * 下面是修改 xl.meta 的逻辑
	 * type FileInfo struct {
	 *		Volume string
	 *		Name string
	 *		VersionID string
	 *		IsLatest bool
	 *		Deleted bool
	 *		DataDir string
	 *		XLV1 bool
	 *		ModTime time.Time 	// 当前时间为最后一次修改时间
	 *		Size int64
	 *		Mode os.FileMode
	 * 		Metadata map[string]string
	 *		Parts []ObjectPartInfo
	 * 			ObjectPartInfo{
	 *				Number:     partNumber, // partID 用户参数，表示一个 mutlipart的对象应该是
	 *	 			ETag:       partETag, 	// md5hex 输入源的 MD5
	 *	 			Size:       partSize, 	// 每个分片的大小，包含了校验和
	 *	 			ActualSize: actualSize, // 数据的实际大小
	 *	 		} 
	 *		Erasure ErasureInfo 
	 *	}
	 */
	// Once part is successfully committed, proceed with updating erasure metadata.
	fi.ModTime = UTCNow()

	md5hex := r.MD5CurrentHexString()

	// Add the current part.
	fi.AddObjectPart(partID, md5hex, n, data.ActualSize())

	for i, disk := range onlineDisks {
		if disk == OfflineDisk {
			continue
		}
		partsMetadata[i].Size = fi.Size
		partsMetadata[i].ModTime = fi.ModTime
		partsMetadata[i].Parts = fi.Parts
		partsMetadata[i].Erasure.AddChecksumInfo(ChecksumInfo{
			PartNumber: partID,
			Algorithm:  DefaultBitrotAlgorithm,
			/* 每个 EC 编码分块的校验和 包含了本身的 bitrot 的校验和 */
			Hash:       bitrotWriterSum(writers[i]),
		})
	}

	// Writes update `xl.meta` format for each disk.
	if _, err = writeUniqueFileInfo(ctx, onlineDisks, minioMetaMultipartBucket, uploadIDPath, partsMetadata, writeQuorum); err != nil {
		return pi, toObjectErr(err, minioMetaMultipartBucket, uploadIDPath)
	}

	// Return success.
	return PartInfo{
		PartNumber:   partID,
		ETag:         md5hex,
		LastModified: fi.ModTime,
		Size:         fi.Size,
		ActualSize:   data.ActualSize(),
	}, nil
}

// GetMultipartInfo returns multipart metadata uploaded during newMultipartUpload, used
// by callers to verify object states
// - encrypted
// - compressed
/*
 * 在 complete 之前，获取信息
 * 获取 fi.MetaData 部分，该部分是用户设置的 map
 */
func (er erasureObjects) GetMultipartInfo(ctx context.Context, bucket, object, uploadID string, opts ObjectOptions) (MultipartInfo, error) {
	result := MultipartInfo{
		Bucket:   bucket,
		Object:   object,
		UploadID: uploadID,
	}

	if err := er.checkUploadIDExists(ctx, bucket, object, uploadID); err != nil {
		return result, toObjectErr(err, bucket, object, uploadID)
	}

	uploadIDPath := er.getUploadIDDir(bucket, object, uploadID)

	storageDisks := er.getDisks()

	// Read metadata associated with the object from all disks.
	partsMetadata, errs := readAllFileInfo(ctx, storageDisks, minioMetaMultipartBucket, uploadIDPath, opts.VersionID)

	// get Quorum for this object
	readQuorum, _, err := objectQuorumFromMeta(ctx, er, partsMetadata, errs)
	if err != nil {
		return result, toObjectErr(err, minioMetaMultipartBucket, uploadIDPath)
	}

	reducedErr := reduceWriteQuorumErrs(ctx, errs, objectOpIgnoredErrs, readQuorum)
	if reducedErr == errErasureReadQuorum {
		return result, toObjectErr(reducedErr, minioMetaMultipartBucket, uploadIDPath)
	}

	/* 选出最新时间的那个 */
	_, modTime := listOnlineDisks(storageDisks, partsMetadata, errs)

	// Pick one from the first valid metadata.
	/* 最新的 xl.meta 必须唱过半数，引入一定的随机性选择一个 */
	fi, err := pickValidFileInfo(ctx, partsMetadata, modTime, readQuorum)
	if err != nil {
		return result, err
	}

	result.UserDefined = fi.Metadata
	return result, nil
}

// ListObjectParts - lists all previously uploaded parts for a given
// object and uploadID.  Takes additional input of part-number-marker
// to indicate where the listing should begin from.
//
// Implements S3 compatible ListObjectParts API. The resulting
// ListPartsInfo structure is marshaled directly into XML and
// replied back to the client.
/*
 * 类似 scan 功能，根据 partNumberMarker 返回 一批 parts 的信息
 * 每次最多返回 maxParts 个 ，系统内上线是 10000 个
 */
func (er erasureObjects) ListObjectParts(ctx context.Context, bucket, object, uploadID string, partNumberMarker, maxParts int, opts ObjectOptions) (result ListPartsInfo, e error) {
	if err := er.checkUploadIDExists(ctx, bucket, object, uploadID); err != nil {
		return result, toObjectErr(err, bucket, object, uploadID)
	}

	uploadIDPath := er.getUploadIDDir(bucket, object, uploadID)

	storageDisks := er.getDisks()

	// Read metadata associated with the object from all disks.
	partsMetadata, errs := readAllFileInfo(ctx, storageDisks, minioMetaMultipartBucket, uploadIDPath, "")

	// get Quorum for this object
	_, writeQuorum, err := objectQuorumFromMeta(ctx, er, partsMetadata, errs)
	if err != nil {
		return result, toObjectErr(err, minioMetaMultipartBucket, uploadIDPath)
	}

	reducedErr := reduceWriteQuorumErrs(ctx, errs, objectOpIgnoredErrs, writeQuorum)
	if reducedErr == errErasureWriteQuorum {
		return result, toObjectErr(reducedErr, minioMetaMultipartBucket, uploadIDPath)
	}

	_, modTime := listOnlineDisks(storageDisks, partsMetadata, errs)

	// Pick one from the first valid metadata.
	fi, err := pickValidFileInfo(ctx, partsMetadata, modTime, writeQuorum)
	if err != nil {
		return result, err
	}

	// Populate the result stub.
	result.Bucket = bucket
	result.Object = object
	result.UploadID = uploadID
	result.MaxParts = maxParts
	result.PartNumberMarker = partNumberMarker
	result.UserDefined = fi.Metadata

	// For empty number of parts or maxParts as zero, return right here.
	if len(fi.Parts) == 0 || maxParts == 0 {
		return result, nil
	}

	// Limit output to maxPartsList.
	// maxPartsList = 10000
	if maxParts > maxPartsList {
		maxParts = maxPartsList
	}

	// Only parts with higher part numbers will be listed.
	partIdx := objectPartIndex(fi.Parts, partNumberMarker)
	parts := fi.Parts
	if partIdx != -1 {
		parts = fi.Parts[partIdx+1:]
	}
	count := maxParts
	for _, part := range parts {
		result.Parts = append(result.Parts, PartInfo{
			PartNumber:   part.Number,
			ETag:         part.ETag,
			LastModified: fi.ModTime,
			Size:         part.Size,
		})
		count--
		if count == 0 {
			break
		}
	}
	// If listed entries are more than maxParts, we set IsTruncated as true.
	if len(parts) > len(result.Parts) {
		result.IsTruncated = true
		// Make sure to fill next part number marker if IsTruncated is
		// true for subsequent listing.
		nextPartNumberMarker := result.Parts[len(result.Parts)-1].PartNumber
		result.NextPartNumberMarker = nextPartNumberMarker
	}
	return result, nil
}

// CompleteMultipartUpload - completes an ongoing multipart
// transaction after receiving all the parts indicated by the client.
// Returns an md5sum calculated by concatenating all the individual
// md5sums of all the parts.
//
// Implements S3 compatible Complete multipart API.
/*
 * type CompletePart struct {
 *		PartNumber int 	// 1 - 10000
 *		ETag string
 *	}
 *
 * 作用：
 * 	1. 完成分片上传（每片 P 内部可以有若干 PP（10Mib），PP 单独 EC 编码，实际数据大小最小 5M 除了最后一个 P）
 * 	2. 更新 xl.meta，根据 xl.meta 转换为 ObjectInfo 返回
 * 	3. 从 mulitypart 目录移动到真实的最终目录
 * 
 * 逻辑：
 * 	1. 目录 rename
 * 		/diskpath/.minio.sys/multipart/sha256hash(bucket/object)/uploadID(ID)/${fi.DataDir}
 * 		/diskpath/object/${fi.DataDir}
 *
 * 		/diskpath/.minio.sys/multipart/sha256hash(bucket/object)/uploadID(ID)/xl.meta
 * 		/diskpath/object/xl.meta
 * 	
 * 	type FileInfo struct {
 *		Volume string
 *		Name string
 *		VersionID string
 *		IsLatest bool
 *		Deleted bool
 *		DataDir string
 *		XLV1 bool
 *		ModTime time.Time 		// 改成当前时间
 *		Size int64  			// 编码之后所有 part 的总大小
 *		Mode os.FileMode
 *		Metadata map[string]string 	// 里面会有用户数据的真实大小一项，还有所有 part 的校验和
 *		Parts []ObjectPartInfo
 *			ObjectPartInfo{
 *				Number:     partNumber, // partID 用户参数，表示一个 mutlipart的对象应该是
 *	 			ETag:       partETag, 	// md5hex 输入源的 MD5
 *	 			Size:       partSize, 	// 每个分片的大小，包含了校验和
 *	 			ActualSize: actualSize, // 数据的实际大小
 *	 		} 
 *		Erasure ErasureInfo
 *	}
 */
func (er erasureObjects) CompleteMultipartUpload(ctx context.Context, bucket string, object string, uploadID string, parts []CompletePart, opts ObjectOptions) (oi ObjectInfo, e error) {
	if err := er.checkUploadIDExists(ctx, bucket, object, uploadID); err != nil {
		return oi, toObjectErr(err, bucket, object, uploadID)
	}

	// Check if an object is present as one of the parent dir.
	// -- FIXME. (needs a new kind of lock).
	/*
	 * 先判断是否有 /bucket/object 这个路径，有的话就代表已经有同名对象了
	 * 最终大对象也会想其他路径一样转入正常的路径中
	 */
	if er.parentDirIsObject(ctx, bucket, path.Dir(object)) {
		return oi, toObjectErr(errFileParentIsFile, bucket, object)
	}

	/* 通知另外一个模块当前路径有更新 */
	defer ObjectPathUpdated(path.Join(bucket, object))

	// Calculate s3 compatible md5sum for complete multipart.
	s3MD5 := getCompleteMultipartMD5(parts)

	uploadIDPath := er.getUploadIDDir(bucket, object, uploadID)

	storageDisks := er.getDisks()

	// Read metadata associated with the object from all disks.
	partsMetadata, errs := readAllFileInfo(ctx, storageDisks, minioMetaMultipartBucket, uploadIDPath, "")

	// get Quorum for this object
	_, writeQuorum, err := objectQuorumFromMeta(ctx, er, partsMetadata, errs)
	if err != nil {
		return oi, toObjectErr(err, bucket, object)
	}

	reducedErr := reduceWriteQuorumErrs(ctx, errs, objectOpIgnoredErrs, writeQuorum)
	if reducedErr == errErasureWriteQuorum {
		return oi, toObjectErr(reducedErr, bucket, object)
	}

	onlineDisks, modTime := listOnlineDisks(storageDisks, partsMetadata, errs)

	// Calculate full object size.
	var objectSize int64

	// Calculate consolidated actual size.
	var objectActualSize int64

	// Pick one from the first valid metadata.
	fi, err := pickValidFileInfo(ctx, partsMetadata, modTime, writeQuorum)
	if err != nil {
		return oi, err
	}

	// Order online disks in accordance with distribution order.
	onlineDisks = shuffleDisks(onlineDisks, fi.Erasure.Distribution)

	// Order parts metadata in accordance with distribution order.
	partsMetadata = shufflePartsMetadata(partsMetadata, fi.Erasure.Distribution)

	// Save current erasure metadata for validation.
	var currentFI = fi

	// Allocate parts similar to incoming slice.
	fi.Parts = make([]ObjectPartInfo, len(parts))

	// Validate each part and then commit to disk.
	for i, part := range parts {
		partIdx := objectPartIndex(currentFI.Parts, part.PartNumber)
		// All parts should have same part number.
		if partIdx == -1 {
			invp := InvalidPart{
				PartNumber: part.PartNumber,
				GotETag:    part.ETag,
			}
			return oi, invp
		}

		// ensure that part ETag is canonicalized to strip off extraneous quotes
		part.ETag = canonicalizeETag(part.ETag)
		if currentFI.Parts[partIdx].ETag != part.ETag {
			invp := InvalidPart{
				PartNumber: part.PartNumber,
				ExpETag:    currentFI.Parts[partIdx].ETag,
				GotETag:    part.ETag,
			}
			return oi, invp
		}

		// All parts except the last part has to be atleast 5MB.
		if (i < len(parts)-1) && !isMinAllowedPartSize(currentFI.Parts[partIdx].ActualSize) {
			return oi, PartTooSmall{
				PartNumber: part.PartNumber,
				PartSize:   currentFI.Parts[partIdx].ActualSize,
				PartETag:   part.ETag,
			}
		}

		// Save for total object size.
		objectSize += currentFI.Parts[partIdx].Size

		// Save the consolidated actual size.
		objectActualSize += currentFI.Parts[partIdx].ActualSize

		// Add incoming parts.
		fi.Parts[i] = ObjectPartInfo{
			Number:     part.PartNumber,
			Size:       currentFI.Parts[partIdx].Size,
			ActualSize: currentFI.Parts[partIdx].ActualSize,
		}
	}

	// Save the final object size and modtime.
	fi.Size = objectSize
	fi.ModTime = opts.MTime
	if opts.MTime.IsZero() {
		fi.ModTime = UTCNow()
	}

	// Save successfully calculated md5sum.
	fi.Metadata["etag"] = s3MD5

	// Save the consolidated actual size.
	fi.Metadata[ReservedMetadataPrefix+"actual-size"] = strconv.FormatInt(objectActualSize, 10)

	// Update all erasure metadata, make sure to not modify fields like
	// checksum which are different on each disks.
	for index := range partsMetadata {
		partsMetadata[index].Size = fi.Size
		partsMetadata[index].ModTime = fi.ModTime
		partsMetadata[index].Metadata = fi.Metadata
		partsMetadata[index].Parts = fi.Parts
	}

	// Write final `xl.meta` at uploadID location
	if onlineDisks, err = writeUniqueFileInfo(ctx, onlineDisks, minioMetaMultipartBucket, uploadIDPath, partsMetadata, writeQuorum); err != nil {
		return oi, toObjectErr(err, minioMetaMultipartBucket, uploadIDPath)
	}

	// Remove parts that weren't present in CompleteMultipartUpload request.
	for _, curpart := range currentFI.Parts {
		if objectPartIndex(fi.Parts, curpart.Number) == -1 {
			// Delete the missing part files. e.g,
			// Request 1: NewMultipart
			// Request 2: PutObjectPart 1
			// Request 3: PutObjectPart 2
			// Request 4: CompleteMultipartUpload --part 2
			// N.B. 1st part is not present. This part should be removed from the storage.
			er.removeObjectPart(bucket, object, uploadID, fi.DataDir, curpart.Number)
		}
	}

	// Rename the multipart object to final location.
	if onlineDisks, err = renameData(ctx, onlineDisks, minioMetaMultipartBucket, uploadIDPath,
		fi.DataDir, bucket, object, writeQuorum, nil); err != nil {
		return oi, toObjectErr(err, bucket, object)
	}

	// Check if there is any offline disk and add it to the MRF list
	for i, disk := range onlineDisks {
		if disk == nil || storageDisks[i] == nil {
			er.addPartial(bucket, object, fi.VersionID)
			break
		}
	}

	for i := 0; i < len(onlineDisks); i++ {
		if onlineDisks[i] == nil {
			continue
		}
		// Object info is the same in all disks, so we can pick
		// the first meta from online disk
		fi = partsMetadata[i]
		break
	}

	// Success, return object info.
	return fi.ToObjectInfo(bucket, object), nil
}

// AbortMultipartUpload - aborts an ongoing multipart operation
// signified by the input uploadID. This is an atomic operation
// doesn't require clients to initiate multiple such requests.
//
// All parts are purged from all disks and reference to the uploadID
// would be removed from the system, rollback is not possible on this
// operation.
//
// Implements S3 compatible Abort multipart API, slight difference is
// that this is an atomic idempotent operation. Subsequent calls have
// no affect and further requests to the same uploadID would not be honored.
/*
 * /diskpath/.minio.sys/multipart/sha256hash(bucket/object)/uploadID(ID)
 * 删除上述路径下面全部的内容
 *
 * 注意：没有回滚机制，操作不可逆
 */
func (er erasureObjects) AbortMultipartUpload(ctx context.Context, bucket, object, uploadID string) error {
	// Validates if upload ID exists.
	if err := er.checkUploadIDExists(ctx, bucket, object, uploadID); err != nil {
		return toObjectErr(err, bucket, object, uploadID)
	}

	uploadIDPath := er.getUploadIDDir(bucket, object, uploadID)

	// Read metadata associated with the object from all disks.
	partsMetadata, errs := readAllFileInfo(ctx, er.getDisks(), minioMetaMultipartBucket, uploadIDPath, "")

	// get Quorum for this object
	_, writeQuorum, err := objectQuorumFromMeta(ctx, er, partsMetadata, errs)
	if err != nil {
		return toObjectErr(err, bucket, object, uploadID)
	}

	// Cleanup all uploaded parts.
	if err = er.deleteObject(ctx, minioMetaMultipartBucket, uploadIDPath, writeQuorum); err != nil {
		return toObjectErr(err, bucket, object, uploadID)
	}

	// Successfully purged.
	return nil
}
