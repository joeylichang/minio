/*
 * MinIO Cloud Storage, (C) 2015, 2016 MinIO, Inc.
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
	"net/http"

	"github.com/gorilla/mux"
)

// Composed function registering routers for only distributed Erasure setup.
func registerDistErasureRouters(router *mux.Router, endpointZones EndpointZones) {
	// Register storage REST router only if its a distributed setup.
	/*
	 * 元数据、文件数据的处理接口
	 */
	registerStorageRESTHandlers(router, endpointZones)

	// Register peer REST router only if its a distributed setup.
	/*
	 * peer 接口处理逻辑
	 */
	registerPeerRESTHandlers(router)

	// Register bootstrap REST router for distributed setups.
	registerBootstrapRESTHandlers(router)

	// Register distributed namespace lock routers.
	/*
	 * 分布式锁的接口
	 */
	registerLockRESTHandlers(router, endpointZones)
}

// List of some generic handlers which are applied for all incoming requests.
var globalHandlers = []MiddlewareFunc{
	// set x-amz-request-id header.
	addCustomHeaders,
	// set HTTP security headers such as Content-Security-Policy.
	addSecurityHeaders,
	// Forward path style requests to actual host in a bucket federated setup.
	setBucketForwardingHandler,
	// Validate all the incoming requests.
	setRequestValidityHandler,
	// Network statistics
	setHTTPStatsHandler,
	// Limits all requests size to a maximum fixed limit
	setRequestSizeLimitHandler,
	// Limits all header sizes to a maximum fixed limit
	setRequestHeaderSizeLimitHandler,
	// Adds 'crossdomain.xml' policy handler to serve legacy flash clients.
	setCrossDomainPolicy,
	// Redirect some pre-defined browser request paths to a static location prefix.
	setBrowserRedirectHandler,
	// Validates if incoming request is for restricted buckets.
	setReservedBucketHandler,
	// Adds cache control for all browser requests.
	setBrowserCacheControlHandler,
	// Validates all incoming requests to have a valid date header.
	setTimeValidityHandler,
	// Validates all incoming URL resources, for invalid/unsupported
	// resources client receives a HTTP error.
	setIgnoreResourcesHandler,
	// Auth handler verifies incoming authorization headers and
	// routes them accordingly. Client receives a HTTP error for
	// invalid/unsupported signatures.
	setAuthHandler,
	// Enforce rules specific for TLS requests
	setSSETLSHandler,
	// filters HTTP headers which are treated as metadata and are reserved
	// for internal use only.
	filterReservedMetadata,
	// Add new handlers here.
}

// configureServer handler returns final handler for the http server.
/*
 * 共计 171 个接口
 * registerDistErasureRouters（58个）
 * 	registerStorageRESTHandlers（28个）
 * 		HealthHandler
 * 		DiskInfoHandler
 * 		CrawlAndGetDataUsageHandler
 * 		MakeVolHandler
 * 		MakeVolBulkHandler
 * 		StatVolHandler
 * 		DeleteVolHandler
 * 		ListVolsHandler
 * 		AppendFileHandler
 * 		WriteAllHandler
 * 		WriteMetadataHandler
 * 		DeleteVersionHandler
 * 		ReadVersionHandler
 * 		RenameDataHandler
 * 		CreateFileHandler
 * 		CheckFileHandler
 * 		CheckPartsHandler
 * 		ReadAllHandler
 * 		ReadFileHandler
 * 		ReadFileStreamHandler
 * 		ListDirHandler
 * 		WalkHandler
 * 		WalkSplunkHandler
 * 		WalkVersionsHandler
 * 		DeleteVersionsHandler
 * 		DeleteFileHandler
 * 		RenameFileHandler
 * 		VerifyFileHandler
 * 	registerPeerRESTHandlers（22个）
 *		HealthHandler
 *		GetLocksHandler
 *		ServerInfoHandler
 *		ProcOBDInfoHandler
 *		MemOBDInfoHandler
 *		OsOBDInfoHandler
 *		DiskHwOBDInfoHandler
 *		CPUOBDInfoHandler
 *		DriveOBDInfoHandler
 *		NetOBDInfoHandler
 *		DispatchNetOBDInfoHandler
 *		CycleServerBloomFilterHandler
 *		DeleteBucketMetadataHandler
 *		LoadBucketMetadataHandler
 *		SignalServiceHandler
 *		ServerUpdateHandler
 *		DeletePolicyHandler
 *		LoadPolicyHandler
 *		LoadPolicyMappingHandler
 *		DeleteUserHandler
 *		DeleteServiceAccountHandler
 *		LoadUserHandler
 *		LoadServiceAccountHandler
 *		LoadGroupHandler
 *		StartProfilingHandler
 *		DownloadProfilingDataHandler
 *		ReloadFormatHandler
 *		TraceHandler
 *		ListenHandler
 *		BackgroundHealStatusHandler
 *		ConsoleLogHandler
 *		GetLocalDiskIDs
 * 	registerBootstrapRESTHandlers（2个）
 * 		HealthHandler
 *		VerifyHandler
 * 	registerLockRESTHandlers（6个）
 *		HealthHandler
 *		LockHandler
 *		RLockHandler
 *		UnlockHandler
 *		RUnlockHandler
 *		ExpiredHandler
 *
 * registerSTSRouter（5个）
 * 	AssumeRole
 * 	AssumeRoleWithJWT
 * 	AssumeRoleWithClientGrants
 * 	AssumeRoleWithWebIdentity
 * 	AssumeRoleWithLDAPIdentity
 *
 * registerAdminRouter（49个）
 * 	ServiceHandler
 * 	ServerUpdateHandler
 * 	ServerInfoHandler
 *	StorageInfoHandler
 *	DataUsageInfoHandler
 * 	HealHandler(3个)
 * 	BackgroundHealStatusHandler
 * 	StartProfilingHandler
 * 	DownloadProfilingHandler
 * 	GetConfigKVHandler
 * 	SetConfigKVHandler
 * 	DelConfigKVHandler
 * 	HelpConfigKVHandler
 * 	ListConfigHistoryKVHandler
 * 	ClearConfigHistoryKVHandler
 * 	RestoreConfigHistoryKVHandler
 * 	GetConfigHandler
 * 	SetConfigHandler
 * 	AddCannedPolicy
 * 	AccountUsageInfoHandler
 *	AddUser
 * 	SetUserStatus
 *	AddServiceAccount
 *	ListServiceAccounts
 *	DeleteServiceAccount
 *	InfoCannedPolicyV2
 *	ListCannedPoliciesV2
 *	InfoCannedPolicy
 *	ListCannedPolicies
 *	RemoveCannedPolicy
 *	SetPolicyForUserOrGroup
 *	RemoveUser
 *	ListUsers
 *	GetUserInfo
 *	UpdateGroupMembers
 *	GetGroup
 *	ListGroups
 *	SetGroupStatus
 *	GetBucketQuotaConfigHandler
 *	PutBucketQuotaConfigHandler
 *	GetBucketTargetsHandler
 *	GetBucketTargetARNHandler
 *	SetBucketTargetHandler
 *	TopLocksHandler
 *	TraceHandler
 *	ConsoleLogHandler
 *	KMSCreateKeyHandler
 *	KMSKeyStatusHandler
 *	OBDInfoHandler
 *
 * registerHealthCheckRouter（3个）
 *	ClusterCheckHandler
 *	LivenessCheckHandler（2个）
 *	ReadinessCheckHandler（2个）
 *
 * registerMetricsRouter（1个）
 *	metricsHandler
 * 
 * registerAPIRouter（65个）
 * 	HeadObjectHandler
 *	CopyObjectPartHandler
 * 	PutObjectPartHandler
 * 	ListObjectPartsHandler
 * 	CompleteMultipartUploadHandler
 * 	NewMultipartUploadHandler
 * 	AbortMultipartUploadHandler
 * 	GetObjectACLHandler
 * 	PutObjectACLHandler
 * 	GetObjectTaggingHandler
 * 	PutObjectTaggingHandler
 * 	DeleteObjectTaggingHandler
 * 	SelectObjectContentHandler
 * 	GetObjectRetentionHandler
 * 	GetObjectLegalHoldHandler
 * 	GetObjectHandler
 * 	CopyObjectHandler
 * 	PutObjectRetentionHandler
 * 	PutObjectLegalHoldHandler
 * 	PutObjectHandler
 * 	DeleteObjectHandler
 * 	GetBucketLocationHandler
 * 	GetBucketPolicyHandler
 * 	GetBucketLifecycleHandler
 * 	GetBucketEncryptionHandler
 * 	GetBucketObjectLockConfigHandler
 * 	GetBucketReplicationConfigHandler
 * 	GetBucketVersioningHandler
 * 	GetBucketNotificationHandler
 * 	ListenNotificationHandler
 * 	GetBucketACLHandler
 * 	PutBucketACLHandler
 * 	GetBucketCorsHandler
 * 	GetBucketWebsiteHandler
 * 	GetBucketAccelerateHandler
 * 	GetBucketRequestPaymentHandler
 * 	GetBucketLoggingHandler
 * 	GetBucketLifecycleHandler
 * 	GetBucketTaggingHandler
 * 	DeleteBucketWebsiteHandler
 * 	DeleteBucketTaggingHandler
 * 	ListMultipartUploadsHandler
 * 	ListObjectsV2MHandler
 * 	ListObjectsV2Handler
 * 	ListObjectVersionsHandler
 * 	ListObjectsV1Handler
 * 	PutBucketLifecycleHandler
 * 	PutBucketReplicationConfigHandler
 * 	PutBucketEncryptionHandler
 * 	PutBucketPolicyHandler
 * 	PutBucketObjectLockConfigHandler
 * 	PutBucketTaggingHandler
 * 	PutBucketVersioningHandler
 * 	PutBucketNotificationHandler
 * 	PutBucketHandler
 * 	HeadBucketHandler
 * 	PostPolicyBucketHandler
 * 	DeleteMultipleObjectsHandler
 * 	DeleteBucketPolicyHandler
 * 	DeleteBucketReplicationConfigHandler
 * 	DeleteBucketLifecycleHandler
 * 	DeleteBucketEncryptionHandler
 * 	DeleteBucketHandler
 * 	ListenNotificationHandler
 * 	ListBucketsHandler（2个）	
 */
func configureServerHandler(endpointZones EndpointZones) (http.Handler, error) {
	// Initialize router. `SkipClean(true)` stops gorilla/mux from
	// normalizing URL path minio/minio#3256
	router := mux.NewRouter().SkipClean(true).UseEncodedPath()

	// Initialize distributed NS lock.
	if globalIsDistErasure {
		registerDistErasureRouters(router, endpointZones)
	}

	// Add STS router always.
	/*
	 * 角色分配接口
	 */
	registerSTSRouter(router)

	// Add Admin router, all APIs are enabled in server mode.
	registerAdminRouter(router, true, true)

	// Add healthcheck router
	registerHealthCheckRouter(router)

	// Add server metrics router
	registerMetricsRouter(router)

	// Register web router when its enabled.
	if globalBrowserEnabled {
		if err := registerWebRouter(router); err != nil {
			return nil, err
		}
	}

	// Add API router, additionally all server mode support encryption
	// but don't allow SSE-KMS.
	registerAPIRouter(router, true, false)

	router.Use(registerMiddlewares)

	return router, nil
}
