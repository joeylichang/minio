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
		 * 	3. 带有 {...} 时，一个参数视为一个 zone（专门用于扩展集群时的使用模式）
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
	/*
 	 * NotificationSys 主要作用有三个：
 	 * 1. 向集群内其他节点发送消息，进行一些权限类的操作
 	 * 2. bucket 的通知机制的实现
  	 * 3. 诊断的接口（应该是 admin 看一些统计信息使用的）
	 */
	globalNotificationSys = NewNotificationSys(globalEndpoints)

	// Create new bucket metadata system.
	/*
	 * bucket metadata 管理
	 * 落盘在 /diskpath/.minio.sys./buckets/${Bucket}/*.json、*.xml 等文件
	 * 内存中会存储在全局变量中，<bucket, bucket_metadata> map中
	 * 后面很多子系统的配置都是从这里拿出来的
	 */
	globalBucketMetadataSys = NewBucketMetadataSys()

	// Create a new config system.
	/*
	 * 配置管理，是json 的kv 对
	 * 从 ~/.minio/config.json to /diskpath/.minio.sys/config/config.json
	 */
	globalConfigSys = NewConfigSys()

	// Create new IAM system.
	/*
	 * 权限管理
	 */
	globalIAMSys = NewIAMSys()

	// Create new policy system.
	/*
	 * 访问控制，例如：bucket 粒度的创建、删除等操作是否被允许
	 */
	globalPolicySys = NewPolicySys()

	// Create new lifecycle system.
	/*
	 * bucket 的生命周期，过期等
	 */
	globalLifecycleSys = NewLifecycleSys()

	// Create new bucket encryption subsystem
	/*
	 * SSE = Server Side Encryption
	 */
	globalBucketSSEConfigSys = NewBucketSSEConfigSys()

	// Create new bucket object lock subsystem
	globalBucketObjectLockSys = NewBucketObjectLockSys()

	// Create new bucket quota subsystem
	/*
	 * 分为 HardQuto、FIFOQuta（配额不够时，walk一遍删除最老的数据）
	 */
	globalBucketQuotaSys = NewBucketQuotaSys()

	// Create new bucket versioning subsystem
	globalBucketVersioningSys = NewBucketVersioningSys()

	// Create new bucket replication subsytem
	globalBucketReplicationSys = NewBucketReplicationSys()
}

/*
 * 1. 初始化，heal 后端框架（heal_task queue 执行的调度框架）
 * 2. 本次磁盘 heal（3min 周期检查）
 * 	2.1 先检查每块磁盘的 format.json 是否异常，不一致之间跳出循环等待下一个 check 周期
 * 	2.2 如果有异常的磁盘对其上全部的数据进行 heal 检查（调用 healset 接口）
 * 3. 获取全局锁
 * 	3.1 为了保证全局锁不过期（默认 2min 过期时间），会周期性的check
 * 	3.2 获取全局锁失败，则重试，目的是保证所有的节点顺序的执行下面的逻辑
 * 4. 加密配置生成，无限重试等待成功
 * 5. 初始化全部的子系统
 */
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
	/*
	 * /diskpath/.minio.sys/config/transaction.lock
	 */
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
		/*
		 * 遍历所有的 zone、set、disk 获取bucket 并且 一个set 内去重
		 */
		buckets, err = newObject.ListBucketsHeal(ctx)
		if err != nil {
			return fmt.Errorf("Unable to list buckets to heal: %w", err)
		}
		// Attempt a heal if possible and re-use the bucket names
		// to reload their config.
		wquorum := &InsufficientWriteQuorum{}
		rquorum := &InsufficientReadQuorum{}
		for _, bucket := range buckets {
			/*
			 * 对于有的磁盘上 bucket 缺失的进行 makevol
			 * 并且生成器 meta 并持久化
			 */
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

/*
 * 1. 数据副本修复
 * 2. 数据统计
 *
 * 注意：全局唯一，只有leader 能够做这个事情
 */
func startBackgroundOps(ctx context.Context, objAPI ObjectLayer) {
	// Make sure only 1 crawler is running on the cluster.
	locker := objAPI.NewNSLock(ctx, minioMetaBucket, "leader")
	for {
		err := locker.GetLock(leaderLockTimeout)
		if err != nil {
			/*
			 * 1 hour 
			 */
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
/*
 * 总结：
 * 	1. 参数解析
 * 	2. http handler 注册，与 http server 启动
 * 	3. 子系统初始化，并且赋值给全局变量	
 * 
 * 具体：
 * 1. 参数解析 && 全局变量初始化
 * 	1.1 主要是 Endpoints（Host:Dir）解析
 * 		1.1.1 zone、set、EC编码分块数量等解析与计算
 * 	1.2 环境变量中的配置将会覆盖之前的参数配置值
 * 2. 在线升级程序检查、系统资源设置（内存、fd等）
 * 3. Heal 全局变量生成
 * 	3.1 globalAllHealState
 * 		admin 中 HealHandler 接口使用的 heal 变量
 * 	3.2 globalBackgroundHealState
 * 		全局后台运行的 heal 模块
 * 	3.3 allHealState
 * 		上述两个变量，都是 allHealState 系统的实例化
 * 		3.3.1 allHealState 内部维护一个路径 与 heal_task 的map
 * 		3.3.2 协理机制
 *			内部启动一个协程周期（5min）检查map 内所有的任务是否结束或者停止，是的话从map中清除
 * 		3.3.3 加入机制
 * 			3.3.3.1 task_task 是结构 healSequence
 * 			3.3.3.2 LaunchNewHealSequence 接口负责检验（是否存在等），加入map，然后启动相应的 heal_task
 * 		3.3.4 读取、停止等机制（有接口能够进行查询和控制）
 * 4. http 接口回调的注册（内部分很多类型），并启动 http 服务
 * 	纵向串联起所有的子模块，后续详细介绍内部逻辑
 * 5. newObjectLayer 初始化
 * 		newObjectLayer 是 zone、set、object 的抽象
 * 		逐层调用（zone、set、object），最后调用 xlstorage 的接口完成数据真正的读写
 * 		xlstorage 抽象为 disk，分位本地磁盘和远程磁盘客户端两种，完成本地和分布式的磁盘操作
 * 		xlstorage 是直接 IO，提升性能
 * 6. 生成所有的子系统对象（全局变量）， newAllSubsystems
 * 		globalNotificationSys
 * 		  BucketMetadata 的配置之一
 * 			1. 向集群内其他节点发送消息，进行一些权限类的操作
 *			2. bucket 的通知机制的实现（删除更新）
 * 			3. 诊断的接口（应该是 admin 看一些统计信息使用的，内存，磁盘等）
 * 		globalBucketMetadataSys
 * 			1. bucket metadata 管理
 * 			2. 落盘在 /diskpath/.minio.sys./buckets/${Bucket}/*.json、*.xml 等文件
 * 				十多个元数据文件，与后面很对的子系统相对应
 * 			3. 内存中会存储在全局变量中，<bucket, bucket_metadata> map中
 ******************************************************************
 * 		globalConfigSys
 * 			1. 对应的文件：/diskpath/.minio.sys/config/config.json
 * 			2. 配置管理，是json 的kv 对
 * 		globalIAMSys
 * 			权限管理模块
 * 		注意：只有这两个子系统的配置不在 BucketMetadata 中
 ******************************************************************
 * 		globalPolicySys
 * 		  BucketMetadata 的配置之一
 *			访问控制，例如：bucket 粒度的创建、删除等操作是否被允许
 * 		globalLifecycleSys
 *        BucketMetadata 的配置之一
 * 			bucket 的生命周期，过期等
 * 		globalBucketSSEConfigSys
 * 		  BucketMetadata 的配置之一
 * 			SSE = Server Side Encryption
 * 		globalBucketObjectLockSys
 * 	      BucketMetadata 的配置之一
 * 			userDefine 的 metadata 部分
 * 			主要是兼容 S3 语义，自定一些对象是否可删除的配置，内部兼容S3的校验逻辑
 * 		globalBucketQuotaSys
 *   	  BucketMetadata 的配置之一
 * 			分为 HardQuto、FIFOQuta（配额不够时，walk一遍删除最老的数据
 * 		globalBucketVersioningSys
 * 		  BucketMetadata 的配置之一
 * 			一种类似 leveldb 的 mainfest 管理机制
 * 			每一种配置的变更都是向源文件中追加修改记录
 * 			globalBucketMetadataSys 判断当前 version 的状态是 enable 还是 suspend
 * 		globalBucketReplicationSys
 * 		  BucketMetadata 的配置之一
 * 			应该是根据配置进行副本写的写入
 * 			内部很多兼容 S3 的语义
 * 7. 启动 background 任务， startBackgroundOps
 * 	注意： startBackgroundOps 只有集群 leader 能够执行该部分逻辑
 * 		7.1 数据副本修复
 * 			启动 globalBackgroundHealState 后台任务
 * 		7.2 DataUsage（目录关系组织的对象大小、磁盘大小等统计信息） 数据统计
 *			7.2.1 落盘 /diskpath/.minio.sys/buckets/.usage.json
 *			7.2.2 周期性的更新 boolmfilter
 * 8. 所有节点顺序安全的初始化：子系统 ， initSafeMode
 * 		8.1 初始化，heal 后端框架（heal_task queue 执行的调度框架）
 * 		8.2 本次磁盘 heal（3min 周期检查）
 * 			8.2.1 先检查每块磁盘的 format.json 是否异常，不一致之间跳出循环等待下一个 check 周期
 * 			8.2.2 如果有异常的磁盘对其上全部的数据进行 heal 检查（调用 healset 接口）
 * 		8.3 分布式锁保证所有节点顺序执行，并且一定执行
 * 			8.3.1 加密配置生成，无限重试等待成功
 * 			8.3.2 初始化全部的子系统
 * 		注意：
 * 			为了保证全局锁不过期（默认 2min 过期时间），会周期性的check
 * 			获取全局锁失败，则重试，目的是保证所有的节点顺序的执行下面的逻辑
 * 9. 初始化权限系统， startBackgroundIAMLoad
 * 10. 初始化 disk cache for object， newServerCacheObjects
 */
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

	/*
	 * endpoint 中的节点，去重同一个 host 上面多个目录的重复，组成新的数据结构 []ProxyEndpoint
	 */
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
		/*
		 * 从官网查看是否需要更新程序，如果需要的话会打印一条日志输出到控制台
		 */
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
	/*
	 * 各种 http api 注册，重要的是 S3 接口 
	 */
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
