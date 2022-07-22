// Copyright 2017-2021 DERO Project. All rights reserved.
// Use of this source code in any form is governed by RESEARCH license.
// license can be found in the LICENSE file.
// GPG: 0F39 E425 8C65 3947 702A  8234 08B2 0360 A03A 9DE8
//
//
// THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND ANY
// EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF
// MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL
// THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
// SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO,
// PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
// INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT,
// STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF
// THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

package block

import (
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/deroproject/derohe/config"
)

type MiniBlocksCollection struct {
	Collection map[MiniBlockKey][]MiniBlock
	sync.RWMutex
}

// create a collection
func CreateMiniBlockCollection() *MiniBlocksCollection {
	return &MiniBlocksCollection{Collection: map[MiniBlockKey][]MiniBlock{}}
}

var orphan_block_count_mutex sync.Mutex

var OrphanMiniCount int

var miner_mini_mutex sync.Mutex

var MyBlocks = make(map[string][]MiniBlock)
var MyOrphanBlocks = make(map[string][]MiniBlock)

func AddBlockToMyCollection(mbl MiniBlock, miner string) {
	miner_mini_mutex.Lock()
	defer miner_mini_mutex.Unlock()

	MyBlocks[miner] = append(MyBlocks[miner], mbl)

}

var LastPurgeHeight uint64 = 0
var MiniBlockCounterMap = make(map[string]time.Time)
var OrphanMiniCounterMap = make(map[string]time.Time)
var OrphanMiniCounter100 = make(map[string]int64)
var OrphanBlocks = make(map[string]int64)

func GetMyOrphansList() map[string]map[string]uint64 {

	miner_mini_mutex.Lock()
	defer miner_mini_mutex.Unlock()

	var OrphanList = make(map[string]map[string]uint64)

	for miner, _ := range MyOrphanBlocks {

		var stat = make(map[string]uint64)

		for _, mbl := range MyOrphanBlocks[miner] {

			stat[mbl.GetHash().String()] = mbl.Height

			OrphanList[miner] = stat
		}
	}

	return OrphanList
}

func GetMinerOrphanCount(Address string) uint64 {

	miner_mini_mutex.Lock()
	defer miner_mini_mutex.Unlock()

	_, found := MyOrphanBlocks[Address]
	if found {
		return uint64(len(MyOrphanBlocks[Address]))
	}

	return 0
}

func BlockRateCount(height int64) (int, int, float64, int) {

	orphan_block_count_mutex.Lock()
	defer orphan_block_count_mutex.Unlock()

	for x := range OrphanBlocks {
		if OrphanBlocks[x]+config.RunningConfig.NetworkStatsKeepCount < height {
			delete(OrphanBlocks, x)
		}
	}

	for x := range OrphanMiniCounter100 {
		if OrphanMiniCounter100[x]+100 < height {
			delete(OrphanMiniCounter100, x)
		}
	}

	// return amount of lost mini's in last 10 min
	for x, i := range OrphanMiniCounterMap {
		if i.Unix()+600 < time.Now().Unix() {
			delete(OrphanMiniCounterMap, x)
		}
	}

	for x, i := range MiniBlockCounterMap {
		if i.Unix()+600 < time.Now().Unix() {
			delete(MiniBlockCounterMap, x)
		}
	}

	orphan_rate := float64(0)
	if len(OrphanMiniCounterMap) > 0 {
		orphan_rate = float64(float64(float64(len(OrphanMiniCounterMap))/float64(len(MiniBlockCounterMap))) * 100)
	}

	return len(OrphanMiniCounterMap), len(MiniBlockCounterMap), orphan_rate, len(OrphanMiniCounter100)
}

func IsBlockOrphan(block_hash string) bool {

	orphan_block_count_mutex.Lock()
	defer orphan_block_count_mutex.Unlock()

	_, x := OrphanBlocks[block_hash]

	return x

}

// purge all heights less than this height
func (c *MiniBlocksCollection) PurgeHeight(minis []MiniBlock, height int64) (purge_count int) {
	if height < 0 {
		return
	}
	c.Lock()
	defer c.Unlock()

	orphan_block_count_mutex.Lock()
	defer orphan_block_count_mutex.Unlock()

	for k, _ := range c.Collection {
		if k.Height <= uint64(height) {
			purge_count++

			if minis != nil {
				toPurge := c.Collection[k]
				matches := 0
				for _, mbl := range toPurge {
					match := false
					for _, mbl2 := range minis {
						if mbl.Height == mbl2.Height && mbl.Timestamp == mbl2.Timestamp &&
							mbl.Final == mbl2.Final {
							match = true
							for i := 0; i < 16; i++ {
								if mbl.KeyHash[i] != mbl2.KeyHash[i] {
									match = false
									break
								}
							}
							if match {
								matches++
								break
							}
						}
					}

					if !match {

						// Log lost minis

						i := time.Now()
						OrphanMiniCounterMap[mbl.GetHash().String()] = i
						OrphanMiniCounter100[mbl.GetHash().String()] = height
						OrphanBlocks[mbl.GetHash().String()] = height

						miner_mini_mutex.Lock()
						// Check my minis, if any are orphaned
						for miner := range MyBlocks {
							for _, my_mbl := range MyBlocks[miner] {
								if my_mbl.GetHash() == mbl.GetHash() {
									MyOrphanBlocks[miner] = append(MyOrphanBlocks[miner], my_mbl)
									break
								}
							}
						}
						miner_mini_mutex.Unlock()

					}
				}
				OrphanMiniCount += len(toPurge) - matches
			}

			delete(c.Collection, k)
		}
	}

	LastPurgeHeight = uint64(height)

	return purge_count
}

func (c *MiniBlocksCollection) Count() int {
	c.RLock()
	defer c.RUnlock()
	count := 0
	for _, v := range c.Collection {
		count += len(v)
	}

	return count
}

// check if already inserted
func (c *MiniBlocksCollection) IsAlreadyInserted(mbl MiniBlock) bool {
	return c.IsCollision(mbl)
}

// check if collision will occur
func (c *MiniBlocksCollection) IsCollision(mbl MiniBlock) bool {
	c.RLock()
	defer c.RUnlock()

	return c.isCollisionnolock(mbl)
}

// this assumes that we are already locked
func (c *MiniBlocksCollection) isCollisionnolock(mbl MiniBlock) bool {
	mbls := c.Collection[mbl.GetKey()]
	for i := range mbls {
		if mbl == mbls[i] {
			return true
		}
	}
	return false
}

// insert a miniblock
func (c *MiniBlocksCollection) InsertMiniBlock(mbl MiniBlock) (err error, result bool) {

	if mbl.Final {

		return fmt.Errorf("Final cannot be inserted"), false
	}

	c.Lock()
	defer c.Unlock()

	if c.isCollisionnolock(mbl) {
		return fmt.Errorf("collision %x", mbl.Serialize()), false
	}

	orphan_block_count_mutex.Lock()
	i := time.Now()
	MiniBlockCounterMap[mbl.GetHash().String()] = i
	orphan_block_count_mutex.Unlock()

	c.Collection[mbl.GetKey()] = append(c.Collection[mbl.GetKey()], mbl)
	return nil, true
}

// get all the genesis blocks
func (c *MiniBlocksCollection) GetAllMiniBlocks(key MiniBlockKey) (mbls []MiniBlock) {
	c.RLock()
	defer c.RUnlock()

	for _, mbl := range c.Collection[key] {
		mbls = append(mbls, mbl)
	}
	return
}

// get all the tips from the map, this is atleast O(n)
func (c *MiniBlocksCollection) GetAllKeys(height int64) (keys []MiniBlockKey) {
	c.RLock()
	defer c.RUnlock()

	for k := range c.Collection {
		if k.Height == uint64(height) {
			keys = append(keys, k)
		}
	}

	sort.SliceStable(keys, func(i, j int) bool { // sort descending on the basis of work done
		return len(c.Collection[keys[i]]) > len(c.Collection[keys[j]])
	})

	return
}
