/*
 * MinIO Cloud Storage, (C) 2015-2019 MinIO, Inc.
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
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/minio/cli"
	"github.com/minio/minio/cmd/config"
	xhttp "github.com/minio/minio/cmd/http"
	"github.com/minio/minio/cmd/logger"
	"github.com/minio/minio/pkg/auth"
	"github.com/minio/minio/pkg/certs"
	"github.com/minio/minio/pkg/color"
	"github.com/minio/minio/pkg/env"
	"github.com/minio/minio/pkg/retry"
)

// ServerFlags - server command specific flags
var ServerFlags = []cli.Flag{
	cli.StringFlag{
		Name:  "address",
		Value: ":" + GlobalMinioDefaultPort,
		Usage: "bind to a specific ADDRESS:PORT, ADDRESS can be an IP or hostname",
	},
}

var serverCmd = cli.Command{
	Name:   "server",
	Usage:  "start object storage server",
	Flags:  append(ServerFlags, GlobalFlags...),
	Action: serverMain,
	CustomHelpTemplate: `NAME:
  {{.HelpName}} - {{.Usage}}

USAGE:
  {{.HelpName}} {{if .VisibleFlags}}[FLAGS] {{end}}DIR1 [DIR2..]
  {{.HelpName}} {{if .VisibleFlags}}[FLAGS] {{end}}DIR{1...64}
  {{.HelpName}} {{if .VisibleFlags}}[FLAGS] {{end}}DIR{1...64} DIR{65...128}

DIR:
  DIR points to a directory on a filesystem. When you want to combine
  multiple drives into a single large system, pass one directory per
  filesystem separated by space. You may also use a '...' convention
  to abbreviate the directory arguments. Remote directories in a
  distributed setup are encoded as HTTP(s) URIs.
{{if .VisibleFlags}}
FLAGS:
  {{range .VisibleFlags}}{{.}}
  {{end}}{{end}}
EXAMPLES:
  1. Start minio server on "/home/shared" directory.
     {{.Prompt}} {{.HelpName}} /home/shared

  2. Start single node server with 64 local drives "/mnt/data1" to "/mnt/data64".
     {{.Prompt}} {{.HelpName}} /mnt/data{1...64}

  3. Start distributed minio server on an 32 node setup with 32 drives each, run following command on all the nodes
     {{.Prompt}} {{.EnvVarSetCommand}} MINIO_ACCESS_KEY{{.AssignmentOperator}}minio
     {{.Prompt}} {{.EnvVarSetCommand}} MINIO_SECRET_KEY{{.AssignmentOperator}}miniostorage
     {{.Prompt}} {{.HelpName}} http://node{1...32}.example.com/mnt/export{1...32}

  4. Start distributed minio server in an expanded setup, run the following command on all the nodes
     {{.Prompt}} {{.EnvVarSetCommand}} MINIO_ACCESS_KEY{{.AssignmentOperator}}minio
     {{.Prompt}} {{.EnvVarSetCommand}} MINIO_SECRET_KEY{{.AssignmentOperator}}miniostorage
     {{.Prompt}} {{.HelpName}} http://node{1...16}.example.com/mnt/export{1...32} \
            http://node{17...64}.example.com/mnt/export{1...64}
`,
}

// Checks if endpoints are either available through environment
// or command line, returns false if both fails.
func endpointsPresent(ctx *cli.Context) bool {
	endpoints := env.Get(config.EnvEndpoints, strings.Join(ctx.Args(), config.ValueSeparator))
	return len(endpoints) != 0
}

func serverHandleCmdArgs(ctx *cli.Context) {
	// Handle common command args.
	handleCommonCmdArgs(ctx)

	logger.FatalIf(CheckLocalServerAddr(globalCLIContext.Addr), "Unable to validate passed arguments")

	var setupType SetupType
	var err error

	globalMinioAddr = globalCLIContext.Addr

	globalMinioHost, globalMinioPort = mustSplitHostPort(globalMinioAddr)
	endpoints := strings.Fields(env.Get(config.EnvEndpoints, ""))
	if len(endpoints) > 0 {
		globalEndpoints, globalErasureSetDriveCount, setupType, err = createServerEndpoints(globalCLIContext.Addr, endpoints...)
	} else {
		/*
		 * 参数输入约束：
		 * 	1. 要么所有的参数（按空格划分参数）都没有 {...}，要么每一个都得有 {...}
		 * 	2. 内部创建 endpoints（host:dir） 是会要求所有的 endpoint 类型相同，否则启动失败
		 * 		2.1 内部调用 HasEllipses 会一次性校验全部的参数，如果多个参数中，有的有 {...}，有的没有，会按没有处理，那么参数的类型（path or url）将不同，启动失败
		 * 	3. 带有 {...} 时，一个参数视为一个 zone（应该是专门用于扩展集群时的使用模式）
		 * 		3.1 所有的 zone，根据参数计算的 EC 编码块数 和 启动模式（单机、EC、DisEC）必须相同
		 * 		3.2 如果是以 {...} 方式启动的 DisEC 模式集群
		 * 			3.2.1 要求节点必须是同构的
		 * 			3.2.2 扩容时必须也用 {...} 启动，则新加入的节点必须也是同构的
		 * 			3.2.3 但是扩容的节点和元节点可以不同构，只要经过同样的计算逻辑保证 EC 编码块数 和 启动模式 相同即可
		 * 		3.3 如果是以单个目录展开的方式启动的 DisEC 模式集群
		 * 			3.3.1 所有的节点在一个 zone 中，不能进行集群模式扩容（悬疑，带后面解决，因为如果是hash算法的话，取模的总数变了） 
		 * 
		 * 结论：
		 * 	1. zone 的方式可扩展，如果使用的话，要求机器必须同构（磁盘）
		 * 	2. 跨 zone 不需要机器同构，但是需要新的zone 的参数经过计算之后有相同的 EC 编码分块数量，对于新加机器也是有一定要求的
		 * 
		 * 参数解析逻辑：
		 * 	1. 带有 {...} 参数的解析流程（createServerEndpoints）
		 * 		1.1 内部循环所有的输入参数，既每个 zone 的参数
		 * 		1.2 循环内调用 GetAllSets ，完成 EC 编码分块数量的计算，以及 <host, dir> 的分组（分几组，每组内部的路径是什么）
		 * 			1.2.1 totalSizes = host_num * dir_num,
		 * 			1.2.2 求 totalSizes 的最大公约数 commonSize，计算备选集 setCounts = 预值EC编码分块数量（[4, 16]）能被 commonSize 整除的集合
		 * 					如果环境变量设置了 EC 编码的分块数量，此时必须在 setCounts 中，否则启动失败
		 * 			1.2.3 如果 dir 使用了 {...}， 则从 setCounts 中选出整除或者被整除 dir_num 的备选集 setCounts2
		 * 				  如果 dir 没有使用 {...}，则只有 host 使用 {...} ，则从 setCounts 中选出整除或者被整除 host_num 的备选集 setCounts2
		 * 				  如果 dir 和 host 都使用了 {...}，则用 dir_num
		 * 				注意：核心保证 EC 的数据块在 磁盘上是均匀分布的
		 * 			1.2.4 globalErasureSetDriveCount = max{setCounts2}
		 * 			1.2.5 对 <host, dir> 进行分组，根据 EC 编码的分块数量，先遍历 host，在遍历 dir 完成分组
		 * 		1.3 循环内调用 CreateEndpoints，对 GetAllSets 生成的结果进行处理，然后生成启动模式（FSSetupType、ErasureSetupType、DistErasureSetupType）
		 * 			1.3.1 根据 1.2 中生成的分组初始化 endpointlist，对其进行一些约束检查（类型是否一致、checkCrossDeviceMounts等），否则返回err
		 * 			1.3.2 setupType 选择
		 * 				1.3.2.1 FSSetupType：host 和 dir 都只有一个
		 * 				1.3.2.2 ErasureSetupType：参数中没有host，只有dir，且多余一个
		 * 				1.3.2.3 DistErasureSetupType
		 * 					a. 参数生成的 endpoints 都是 url 类型
		 * 					b. 同一个dir，没有被同一个host  不同的 port 使用，既 dir 不能夸进程共享
		 * 					c. 可能出现，所有的 endpoints 都是本机的url 类型，此时由于使用了 url 算作是 DistErasureSetupType 
		 * 		1.4 循环内会判断前后两个 zone 参数生成的 EC 编码分块数量 和 SetupType 是否相同，否则返回 error
		 * 		1.5 循环内将生成的 <host, dir> 的分组，既 endpointList 加入 endpointZones。
		 * 		1.6 循环外返回， endpointZones，EC 编码分块数，SetupType
		 * 	2. 不带有 {...} 参数的解析流程
		 *		2.1 主要流程与上面完全一致，至少少了 {...} 的解析
		 * 		2.2 totalSizes = len(args)
		 */
		globalEndpoints, globalErasureSetDriveCount, setupType, err = createServerEndpoints(globalCLIContext.Addr, ctx.Args()...)
	}
	logger.FatalIf(err, "Invalid command line arguments")

	// On macOS, if a process already listens on LOCALIPADDR:PORT, net.Listen() falls back
	// to IPv6 address ie minio will start listening on IPv6 address whereas another
	// (non-)minio process is listening on IPv4 of given port.
	// To avoid this error situation we check for port availability.
	logger.FatalIf(checkPortAvailability(globalMinioHost, globalMinioPort), "Unable to start the server")

	globalIsErasure = (setupType == ErasureSetupType)
	globalIsDistErasure = (setupType == DistErasureSetupType)
	if globalIsDistErasure {
		globalIsErasure = true
	}
}

func serverHandleEnvVars() {
	// Handle common environment variables.
	handleCommonEnvVars()
}

func newAllSubsystems() {
	// Create new notification system and initialize notification targets
	globalNotificationSys = NewNotificationSys(globalEndpoints)

	// Create new bucket metadata system.
	globalBucketMetadataSys = NewBucketMetadataSys()

	// Create a new config system.
	globalConfigSys = NewConfigSys()

	// Create new IAM system.
	globalIAMSys = NewIAMSys()

	// Create new policy system.
	globalPolicySys = NewPolicySys()

	// Create new lifecycle system.
	globalLifecycleSys = NewLifecycleSys()

	// Create new bucket encryption subsystem
	globalBucketSSEConfigSys = NewBucketSSEConfigSys()

	// Create new bucket object lock subsystem
	globalBucketObjectLockSys = NewBucketObjectLockSys()

	// Create new bucket quota subsystem
	globalBucketQuotaSys = NewBucketQuotaSys()

	// Create new bucket versioning subsystem
	globalBucketVersioningSys = NewBucketVersioningSys()

	// Create new bucket replication subsytem
	globalBucketReplicationSys = NewBucketReplicationSys()
}

func initSafeMode(ctx context.Context, newObject ObjectLayer) (err error) {
	// Create cancel context to control 'newRetryTimer' go routine.
	retryCtx, cancel := context.WithCancel(ctx)

	// Indicate to our routine to exit cleanly upon return.
	defer cancel()

	// Make sure to hold lock for entire migration to avoid
	// such that only one server should migrate the entire config
	// at a given time, this big transaction lock ensures this
	// appropriately. This is also true for rotation of encrypted
	// content.
	txnLk := newObject.NewNSLock(retryCtx, minioMetaBucket, minioConfigPrefix+"/transaction.lock")
	defer func(txnLk RWLocker) {
		txnLk.Unlock()

		if err != nil {
			var cerr config.Err
			// For any config error, we don't need to drop into safe-mode
			// instead its a user error and should be fixed by user.
			if errors.As(err, &cerr) {
				return
			}

			// Prints the formatted startup message in safe mode operation.
			// Drops-into safe mode where users need to now manually recover
			// the server.
			printStartupSafeModeMessage(getAPIEndpoints(), err)

			// Initialization returned error reaching safe mode and
			// not proceeding waiting for admin action.
			handleSignals()
		}
	}(txnLk)

	// Enable healing to heal drives if possible
	if globalIsErasure {
		initBackgroundHealing(ctx, newObject)
		initLocalDisksAutoHeal(ctx, newObject)
	}

	// ****  WARNING ****
	// Migrating to encrypted backend should happen before initialization of any
	// sub-systems, make sure that we do not move the above codeblock elsewhere.

	// Initializing sub-systems needs a retry mechanism for
	// the following reasons:
	//  - Read quorum is lost just after the initialization
	//    of the object layer.
	//  - Write quorum not met when upgrading configuration
	//    version is needed, migration is needed etc.
	rquorum := InsufficientReadQuorum{}
	wquorum := InsufficientWriteQuorum{}
	for range retry.NewTimer(retryCtx) {
		// let one of the server acquire the lock, if not let them timeout.
		// which shall be retried again by this loop.
		if err = txnLk.GetLock(newDynamicTimeout(1*time.Second, 3*time.Second)); err != nil {
			logger.Info("Waiting for all MinIO sub-systems to be initialized.. trying to acquire lock")
			continue
		}

		// These messages only meant primarily for distributed setup, so only log during distributed setup.
		if globalIsDistErasure {
			logger.Info("Waiting for all MinIO sub-systems to be initialized.. lock acquired")
		}

		// Migrate all backend configs to encrypted backend configs, optionally
		// handles rotating keys for encryption, if there is any retriable failure
		// that shall be retried if there is an error.
		if err = handleEncryptedConfigBackend(newObject); err == nil {
			// Upon success migrating the config, initialize all sub-systems
			// if all sub-systems initialized successfully return right away
			if err = initAllSubsystems(retryCtx, newObject); err == nil {
				// All successful return.
				if globalIsDistErasure {
					// These messages only meant primarily for distributed setup, so only log during distributed setup.
					logger.Info("All MinIO sub-systems initialized successfully")
				}
				return nil
			}
		}

		// One of these retriable errors shall be retried.
		if errors.Is(err, errDiskNotFound) ||
			errors.Is(err, errConfigNotFound) ||
			errors.Is(err, context.Canceled) ||
			errors.Is(err, context.DeadlineExceeded) ||
			errors.As(err, &rquorum) ||
			errors.As(err, &wquorum) ||
			isErrBucketNotFound(err) {
			logger.Info("Waiting for all MinIO sub-systems to be initialized.. possible cause (%v)", err)
			txnLk.Unlock() // Unlock the transaction lock and allow other nodes to acquire the lock if possible.
			continue
		}

		// Any other unhandled return right here.
		return fmt.Errorf("Unable to initialize sub-systems: %w", err)
	}

	// Return an error when retry is canceled or deadlined
	if err = retryCtx.Err(); err != nil {
		return fmt.Errorf("Unable to initialize sub-systems: %w", err)
	}

	// Retry was canceled successfully.
	return errors.New("Initializing sub-systems stopped gracefully")
}

func initAllSubsystems(ctx context.Context, newObject ObjectLayer) (err error) {
	// %w is used by all error returns here to make sure
	// we wrap the underlying error, make sure when you
	// are modifying this code that you do so, if and when
	// you want to add extra context to your error. This
	// ensures top level retry works accordingly.
	var buckets []BucketInfo
	if globalIsDistErasure || globalIsErasure {
		// List buckets to heal, and be re-used for loading configs.
		buckets, err = newObject.ListBucketsHeal(ctx)
		if err != nil {
			return fmt.Errorf("Unable to list buckets to heal: %w", err)
		}
		// Attempt a heal if possible and re-use the bucket names
		// to reload their config.
		wquorum := &InsufficientWriteQuorum{}
		rquorum := &InsufficientReadQuorum{}
		for _, bucket := range buckets {
			if err = newObject.MakeBucketWithLocation(ctx, bucket.Name, BucketOptions{}); err != nil {
				if errors.As(err, &wquorum) || errors.As(err, &rquorum) {
					// Return the error upwards for the caller to retry.
					return fmt.Errorf("Unable to heal bucket: %w", err)
				}
				if _, ok := err.(BucketExists); !ok {
					// ignore any other error and log for investigation.
					logger.LogIf(ctx, err)
					continue
				}
				// Bucket already exists, nothing that needs to be done.
			}
		}
	} else {
		buckets, err = newObject.ListBuckets(ctx)
		if err != nil {
			return fmt.Errorf("Unable to list buckets: %w", err)
		}
	}

	// Initialize config system.
	if err = globalConfigSys.Init(newObject); err != nil {
		return fmt.Errorf("Unable to initialize config system: %w", err)
	}

	// Populate existing buckets to the etcd backend
	if globalDNSConfig != nil {
		// Background this operation.
		go initFederatorBackend(buckets, newObject)
	}

	// Initialize bucket metadata sub-system.
	if err := globalBucketMetadataSys.Init(ctx, buckets, newObject); err != nil {
		return fmt.Errorf("Unable to initialize bucket metadata sub-system: %w", err)
	}

	// Initialize notification system.
	if err = globalNotificationSys.Init(buckets, newObject); err != nil {
		return fmt.Errorf("Unable to initialize notification system: %w", err)
	}

	// Initialize bucket replication sub-system.
	if err = globalBucketReplicationSys.Init(GlobalContext, buckets, newObject); err != nil {
		return fmt.Errorf("Unable to initialize bucket replication sub-system: %w", err)
	}
	return nil
}

func startBackgroundOps(ctx context.Context, objAPI ObjectLayer) {
	// Make sure only 1 crawler is running on the cluster.
	locker := objAPI.NewNSLock(ctx, minioMetaBucket, "leader")
	for {
		err := locker.GetLock(leaderLockTimeout)
		if err != nil {
			time.Sleep(leaderLockTimeoutSleepInterval)
			continue
		}
		break
		// No unlock for "leader" lock.
	}

	if globalIsErasure {
		initGlobalHeal(ctx, objAPI)
	}

	initDataCrawler(ctx, objAPI)
}

// serverMain handler called for 'minio server' command.
func serverMain(ctx *cli.Context) {
	if ctx.Args().First() == "help" || !endpointsPresent(ctx) {
		cli.ShowCommandHelpAndExit(ctx, "server", 1)
	}
	setDefaultProfilerRates()

	// Initialize globalConsoleSys system
	globalConsoleSys = NewConsoleLogger(GlobalContext)

	signal.Notify(globalOSSignalCh, os.Interrupt, syscall.SIGTERM)

	// Handle all server command args.
	serverHandleCmdArgs(ctx)

	// Handle all server environment vars.
	serverHandleEnvVars()

	// Set node name, only set for distributed setup.
	globalConsoleSys.SetNodeName(globalEndpoints)

	// Initialize all help
	initHelp()

	// Check and load TLS certificates.
	var err error
	globalPublicCerts, globalTLSCerts, globalIsSSL, err = getTLSConfig()
	logger.FatalIf(err, "Unable to load the TLS configuration")

	// Check and load Root CAs.
	globalRootCAs, err = config.GetRootCAs(globalCertsCADir.Get())
	logger.FatalIf(err, "Failed to read root CAs (%v)", err)

	globalProxyEndpoints, err = GetProxyEndpoints(globalEndpoints)
	logger.FatalIf(err, "Invalid command line arguments")

	globalMinioEndpoint = func() string {
		host := globalMinioHost
		if host == "" {
			host = sortIPs(localIP4.ToSlice())[0]
		}
		return fmt.Sprintf("%s://%s", getURLScheme(globalIsSSL), net.JoinHostPort(host, globalMinioPort))
	}()

	// Is distributed setup, error out if no certificates are found for HTTPS endpoints.
	if globalIsDistErasure {
		if globalEndpoints.HTTPS() && !globalIsSSL {
			logger.Fatal(config.ErrNoCertsAndHTTPSEndpoints(nil), "Unable to start the server")
		}
		if !globalEndpoints.HTTPS() && globalIsSSL {
			logger.Fatal(config.ErrCertsAndHTTPEndpoints(nil), "Unable to start the server")
		}
	}

	if !globalCLIContext.Quiet {
		// Check for new updates from dl.min.io.
		checkUpdate(getMinioMode())
	}

	if !globalActiveCred.IsValid() && globalIsDistErasure {
		logger.Fatal(config.ErrEnvCredentialsMissingDistributed(nil),
			"Unable to initialize the server in distributed mode")
	}

	// Set system resources to maximum.
	setMaxResources()

	if globalIsErasure {
		// New global heal state
		globalAllHealState = newHealState()
		globalBackgroundHealState = newHealState()
	}

	// Configure server.
	handler, err := configureServerHandler(globalEndpoints)
	if err != nil {
		logger.Fatal(config.ErrUnexpectedError(err), "Unable to configure one of server's RPC services")
	}

	var getCert certs.GetCertificateFunc
	if globalTLSCerts != nil {
		getCert = globalTLSCerts.GetCertificate
	}

	// Annonying hack to ensure that Go doesn't write its own logging,
	// interleaved with our formatted logging, this allows us to
	// honor --json and --quiet flag properly.
	//
	// Unfortunately we have to resort to this sort of hacky approach
	// because, Go automatically initializes ErrorLog on its own
	// and can log without application control.
	//
	// This is an implementation issue in Go and should be fixed, but
	// until then this hack is okay and works for our needs.
	pr, pw := io.Pipe()
	go func() {
		defer pr.Close()
		scanner := bufio.NewScanner(&contextReader{pr, GlobalContext})
		for scanner.Scan() {
			logger.LogIf(GlobalContext, errors.New(scanner.Text()))
		}
	}()

	httpServer := xhttp.NewServer([]string{globalMinioAddr}, criticalErrorHandler{corsHandler(handler)}, getCert)
	httpServer.ErrorLog = log.New(pw, "", 0)
	httpServer.BaseContext = func(listener net.Listener) context.Context {
		return GlobalContext
	}
	go func() {
		globalHTTPServerErrorCh <- httpServer.Start()
	}()

	globalObjLayerMutex.Lock()
	globalHTTPServer = httpServer
	globalObjLayerMutex.Unlock()

	if globalIsDistErasure && globalEndpoints.FirstLocal() {
		for {
			// Additionally in distributed setup, validate the setup and configuration.
			err := verifyServerSystemConfig(GlobalContext, globalEndpoints)
			if err == nil {
				break
			}
			logger.LogIf(GlobalContext, err, "Unable to initialize distributed setup, retrying.. after 5 seconds")
			select {
			case <-GlobalContext.Done():
				return
			case <-time.After(5 * time.Second):
			}
		}
	}

	newObject, err := newObjectLayer(GlobalContext, globalEndpoints)
	logger.SetDeploymentID(globalDeploymentID)
	if err != nil {
		// Stop watching for any certificate changes.
		globalTLSCerts.Stop()

		globalHTTPServer.Shutdown()
		logger.Fatal(err, "Unable to initialize backend")
	}

	// Once endpoints are finalized, initialize the new object api in safe mode.
	globalObjLayerMutex.Lock()
	globalSafeMode = true
	globalObjectAPI = newObject
	globalObjLayerMutex.Unlock()

	newAllSubsystems()

	go startBackgroundOps(GlobalContext, newObject)

	logger.FatalIf(initSafeMode(GlobalContext, newObject), "Unable to initialize server switching into safe-mode")

	// Initialize users credentials and policies in background.
	go startBackgroundIAMLoad(GlobalContext)

	if globalCacheConfig.Enabled {
		// initialize the new disk cache objects.
		var cacheAPI CacheObjectLayer
		cacheAPI, err = newServerCacheObjects(GlobalContext, globalCacheConfig)
		logger.FatalIf(err, "Unable to initialize disk caching")

		globalObjLayerMutex.Lock()
		globalCacheObjectAPI = cacheAPI
		globalObjLayerMutex.Unlock()
	}

	// Disable safe mode operation, after all initialization is over.
	globalObjLayerMutex.Lock()
	globalSafeMode = false
	globalObjLayerMutex.Unlock()

	// Prints the formatted startup message once object layer is initialized.
	printStartupMessage(getAPIEndpoints())

	if globalActiveCred.Equal(auth.DefaultCredentials) {
		msg := fmt.Sprintf("Detected default credentials '%s', please change the credentials immediately using 'MINIO_ACCESS_KEY' and 'MINIO_SECRET_KEY'", globalActiveCred)
		logger.StartupMessage(color.RedBold(msg))
	}

	handleSignals()
}

// Initialize object layer with the supplied disks, objectLayer is nil upon any error.
func newObjectLayer(ctx context.Context, endpointZones EndpointZones) (newObject ObjectLayer, err error) {
	// For FS only, directly use the disk.
	if endpointZones.NEndpoints() == 1 {
		// Initialize new FS object layer.
		return NewFSObjectLayer(endpointZones[0].Endpoints[0].Path)
	}

	return newErasureZones(ctx, endpointZones)
}
