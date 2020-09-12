/*
 * MinIO Cloud Storage, (C) 2019 MinIO, Inc.
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
	"time"

	"github.com/dustin/go-humanize"
	"github.com/minio/minio/cmd/logger"
)

const defaultMonitorNewDiskInterval = time.Minute * 3

func initLocalDisksAutoHeal(ctx context.Context, objAPI ObjectLayer) {
	go monitorLocalDisksAndHeal(ctx, objAPI)
}

// monitorLocalDisksAndHeal - ensures that detected new disks are healed
//  1. Only the concerned erasure set will be listed and healed
//  2. Only the node hosting the disk is responsible to perform the heal
/*
 * 作用：
 * 	1. Format Heal
 * 	2. Bucket Heal
 *
 * 逻辑：
 * 	1. 只检查本地 disk 是否有 format.json ，没有则收集起来，发送给 heal 模块后端进行修复
 * 		2.1 如果一个zone 中有多个 set，本地的磁盘不一定在所有的 set，那么就有set 没有检查到啊？
 * 	2. 针对这些缺失 format 的磁盘，在reload 之后进行数据 heal
 *
 * 注意： 上述流程周期 3 min
 */
func monitorLocalDisksAndHeal(ctx context.Context, objAPI ObjectLayer) {
	z, ok := objAPI.(*erasureZones)
	if !ok {
		return
	}

	var bgSeq *healSequence
	var found bool

	for {
		bgSeq, found = globalBackgroundHealState.getHealSequenceByToken(bgHealingUUID)
		if found {
			break
		}
		time.Sleep(time.Second)
	}

	// Perform automatic disk healing when a disk is replaced locally.
	for {
		select {
		case <-ctx.Done():
			return
		/*
		 * 1. 周期 3 min
		 */
		case <-time.After(defaultMonitorNewDiskInterval):
			// Attempt a heal as the server starts-up first.
			localDisksInZoneHeal := make([]Endpoints, len(z.zones))
			var healNewDisks bool
			for i, ep := range globalEndpoints {
				localDisksToHeal := Endpoints{}
				for _, endpoint := range ep.Endpoints {
					if !endpoint.IsLocal {
						continue
					}
					// Try to connect to the current endpoint
					// and reformat if the current disk is not formatted
					_, _, err := connectEndpoint(endpoint)
					if err == errUnformattedDisk {
						localDisksToHeal = append(localDisksToHeal, endpoint)
					}
				}
				if len(localDisksToHeal) == 0 {
					continue
				}
				localDisksInZoneHeal[i] = localDisksToHeal
				healNewDisks = true
			}

			// Reformat disks only if needed.
			if !healNewDisks {
				continue
			}

			logger.Info("New unformatted drives detected attempting to heal...")
			for i, disks := range localDisksInZoneHeal {
				for _, disk := range disks {
					logger.Info("Healing disk '%s' on %s zone", disk, humanize.Ordinal(i+1))
				}
			}

			// Reformat disks
			bgSeq.sourceCh <- healSource{bucket: SlashSeparator}

			// Ensure that reformatting disks is finished
			bgSeq.sourceCh <- healSource{bucket: nopHeal}

			var erasureSetInZoneToHeal = make([][]int, len(localDisksInZoneHeal))
			// Compute the list of erasure set to heal
			for i, localDisksToHeal := range localDisksInZoneHeal {
				var erasureSetToHeal []int
				for _, endpoint := range localDisksToHeal {
					// Load the new format of this passed endpoint
					_, format, err := connectEndpoint(endpoint)
					if err != nil {
						printEndpointError(endpoint, err, true)
						continue
					}
					// Calculate the set index where the current endpoint belongs
					setIndex, _, err := findDiskIndex(z.zones[i].format, format)
					if err != nil {
						printEndpointError(endpoint, err, false)
						continue
					}

					erasureSetToHeal = append(erasureSetToHeal, setIndex)
				}
				erasureSetInZoneToHeal[i] = erasureSetToHeal
			}

			// Heal all erasure sets that need
			for i, erasureSetToHeal := range erasureSetInZoneToHeal {
				for _, setIndex := range erasureSetToHeal {
					err := healErasureSet(ctx, setIndex, z.zones[i].sets[setIndex], z.zones[i].drivesPerSet)
					if err != nil {
						logger.LogIf(ctx, err)
					}
				}
			}
		}
	}
}
