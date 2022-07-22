package p2p

import (
	"fmt"
	"regexp"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/deroproject/derohe/block"
	"github.com/deroproject/derohe/blockchain"
	"github.com/deroproject/derohe/config"
	"github.com/deroproject/derohe/globals"
	"github.com/deroproject/derohe/rpc"
)

type BlockInsertCounter struct {
	Blocks_Accepted uint64
	Blocks_Rejected uint64
}

type BlockSendingError struct {
	Block_Type          string
	When                time.Time
	Error_Message       string
	Destination_Peer_ID uint64
}

type BlockReceivingError struct {
	Block_Type    string
	When          time.Time
	Error_Message string
	From_Peer_ID  uint64
}

type BlockCollisionError struct {
	Block_Type    string
	When          time.Time
	Error_Message string
	Peer_ID       uint64
	Incoming      bool
}

type PeerStats struct {
	Sending_Errors   []BlockSendingError
	Receiving_Errors []BlockReceivingError
	Collision_Errors []BlockCollisionError
}

type MyBlockReceivingError struct {
	Block_Type    string
	When          time.Time
	Error_Message string
}

type MiniBlockLog struct {
	Miniblock   block.MiniBlock
	NodeAddress string
	MinerWallet string
	IsOrphan    bool
}

var MiniblockLogs = make(map[string]MiniBlockLog)

type FinalBlockLog struct {
	Block       block.Block
	NodeAddress string
	MinerWallet string
	IsOrphan    bool
}

var FinalBlockLogs = make(map[string]FinalBlockLog)

var Stats_mutex sync.Mutex

var Pstat = make(map[string]PeerStats)
var BlockInsertCount = make(map[string]BlockInsertCounter)

var Selfish_mutex sync.Mutex
var SelfishNodeStats = make(map[string][]MyBlockReceivingError)

var log_miniblock_mutex sync.Mutex

func GetBlockLogLenght() (int, int) {

	log_miniblock_mutex.Lock()
	defer log_miniblock_mutex.Unlock()

	ib := len(FinalBlockLogs)
	mb := len(MiniblockLogs)

	return ib, mb

}

func GetMinerAddressFromKeyHash(chain *blockchain.Blockchain, mbl block.MiniBlock) string {

	if toporecord, err1 := chain.Store.Topo_store.Read(chain.Get_Height()); err1 == nil { // we must now fill in compressed ring members
		if ss, err1 := chain.Store.Balance_store.LoadSnapshot(toporecord.State_Version); err1 == nil {
			if balance_tree, err1 := ss.GetTree(config.BALANCE_TREE); err1 == nil {
				bits, key, _, err1 := balance_tree.GetKeyValueFromHash(mbl.KeyHash[0:16])
				if err1 != nil || bits >= 120 {
					return ""
				}
				if addr, err1 := rpc.NewAddressFromCompressedKeys(key); err1 == nil {
					return addr.String()
				}

			}
		}
	}

	return ""
}

func GetActiveMinersCountFromHeight(height int64) (unique_miner_count int) {

	log_miniblock_mutex.Lock()
	defer log_miniblock_mutex.Unlock()

	var unique_miners = make(map[string]int)

	for _, block := range MiniblockLogs {
		if block.Miniblock.Height >= uint64(height) {
			unique_miners[block.MinerWallet]++
		}
	}

	for _, block := range FinalBlockLogs {
		if block.Block.Height >= uint64(height) {
			unique_miners[block.MinerWallet]++
		}
	}

	return len(unique_miners)
}

func GetNodeFromMiniHash(hash string) (Address string) {

	log_miniblock_mutex.Lock()
	defer log_miniblock_mutex.Unlock()

	for block_hash, block := range MiniblockLogs {

		if hash == block_hash {
			return block.NodeAddress
		}
	}

	for block_hash, block := range FinalBlockLogs {

		if hash == block_hash {
			return block.NodeAddress
		}
	}

	return Address
}

func PotentialNodeIntegratorsFromHeight(height int64, Address string) ([]string, map[string]map[string]float64) {
	log_miniblock_mutex.Lock()
	defer log_miniblock_mutex.Unlock()

	var NodeWallets = make(map[string]int)
	var WalletTotals = make(map[string]int)
	var Nodes = make(map[string]int)
	var Finals = make(map[string]int)

	for _, block := range FinalBlockLogs {
		if block.Block.Height >= uint64(height) {
			WalletTotals[block.MinerWallet]++
			if block.NodeAddress == Address {
				NodeWallets[block.MinerWallet]++
				Nodes[block.NodeAddress]++
				Finals[block.MinerWallet]++
			}
		}
	}

	var ordered_miners []string
	var likelyhood_score = make(map[string]float64)
	for wallet, _ := range NodeWallets {
		ordered_miners = append(ordered_miners, wallet)
		likelyhood_score[wallet] = float64(float64(NodeWallets[wallet]) / float64(WalletTotals[wallet]) * 100)
	}

	sort.SliceStable(ordered_miners, func(i, j int) bool {
		return likelyhood_score[ordered_miners[i]] > likelyhood_score[ordered_miners[j]]
	})

	var data = make(map[string]map[string]float64)

	for wallet := range likelyhood_score {

		_, found := data[wallet]
		if !found {
			data[wallet] = make(map[string]float64)
		}

		d := data[wallet]
		d["likelyhood"] = likelyhood_score[wallet]
		d["finals"] = float64(Finals[wallet])

		data[wallet] = d
	}

	return ordered_miners, data
}

func PotentialMinersOnNodeFromHeight(height int64, Address string) ([]string, map[string]map[string]float64) {
	log_miniblock_mutex.Lock()
	defer log_miniblock_mutex.Unlock()

	var NodeWallets = make(map[string]int)
	var WalletTotals = make(map[string]int)
	var Nodes = make(map[string]int)
	var Minis = make(map[string]int)
	var Finals = make(map[string]int)
	var Orphans = make(map[string]int)

	for _, bl := range FinalBlockLogs {
		if bl.Block.Height >= uint64(height) {
			WalletTotals[bl.MinerWallet]++
			if bl.NodeAddress == Address {
				NodeWallets[bl.MinerWallet]++
				Nodes[bl.NodeAddress]++
				Finals[bl.MinerWallet]++
				if bl.IsOrphan {
					Orphans[bl.MinerWallet]++
				}
			}
		}
	}

	for _, mbl := range MiniblockLogs {
		if mbl.Miniblock.Height >= uint64(height) {
			WalletTotals[mbl.MinerWallet]++
			if mbl.NodeAddress == Address {
				NodeWallets[mbl.MinerWallet]++
				Nodes[mbl.NodeAddress]++
				Minis[mbl.MinerWallet]++
				if mbl.IsOrphan {
					Orphans[mbl.MinerWallet]++
				}
			}
		}
	}

	var ordered_miners []string
	var likelyhood_score = make(map[string]float64)
	for wallet, _ := range NodeWallets {
		ordered_miners = append(ordered_miners, wallet)
		likelyhood_score[wallet] = float64(float64(NodeWallets[wallet]) / float64(WalletTotals[wallet]) * 100)
	}

	sort.SliceStable(ordered_miners, func(i, j int) bool {
		return likelyhood_score[ordered_miners[i]] > likelyhood_score[ordered_miners[j]]
	})

	var data = make(map[string]map[string]float64)

	for wallet := range likelyhood_score {

		_, found := data[wallet]
		if !found {
			data[wallet] = make(map[string]float64)
		}

		d := data[wallet]
		d["likelyhood"] = likelyhood_score[wallet]
		d["minis"] = float64(Minis[wallet])
		d["finals"] = float64(Finals[wallet])
		d["orphans"] = float64(Orphans[wallet])

		data[wallet] = d
	}

	return ordered_miners, data
}

func UpdateLiveBlockData() {

	log_miniblock_mutex.Lock()
	defer log_miniblock_mutex.Unlock()

	for key, mbl := range MiniblockLogs {

		if mbl.Miniblock.Height+uint64(config.RunningConfig.NetworkStatsKeepCount) < uint64(chain.Get_Height()) {
			delete(MiniblockLogs, key)
			continue
		}

		if block.IsBlockOrphan(mbl.Miniblock.GetHash().String()) {
			mbl.IsOrphan = true
		} else {
			mbl.IsOrphan = false
		}
		MiniblockLogs[key] = mbl
	}

	for key, bl := range FinalBlockLogs {

		if bl.Block.Height+uint64(config.RunningConfig.NetworkStatsKeepCount) < uint64(chain.Get_Height()) {
			delete(FinalBlockLogs, key)
			continue
		}

		if block.IsBlockOrphan(bl.Block.GetHash().String()) {
			bl.IsOrphan = true
		} else {
			bl.IsOrphan = false
		}
		FinalBlockLogs[key] = bl

	}

}

func PotentialMinerNodeHeight(height int64, Wallet string) ([]string, map[string]map[string]float64) {
	log_miniblock_mutex.Lock()
	defer log_miniblock_mutex.Unlock()

	var Wallets = make(map[string]int)
	var Nodes = make(map[string]int)
	var Minis = make(map[string]int)
	var Finals = make(map[string]int)
	var Orphans = make(map[string]int)

	for _, bl := range FinalBlockLogs {
		if bl.Block.Height >= uint64(height) && bl.MinerWallet == Wallet {
			Nodes[bl.NodeAddress]++
			Wallets[bl.MinerWallet]++
			Finals[bl.NodeAddress]++
			if bl.IsOrphan {
				Orphans[bl.NodeAddress]++
			}
		}
	}

	for _, mbl := range MiniblockLogs {
		if mbl.Miniblock.Height >= uint64(height) && mbl.MinerWallet == Wallet {
			Wallets[mbl.MinerWallet]++
			Nodes[mbl.NodeAddress]++
			Minis[mbl.NodeAddress]++
			if mbl.IsOrphan {
				Orphans[mbl.NodeAddress]++
			}

		}
	}

	var ordered_nodes []string
	var likelyhood_score = make(map[string]float64)
	for node, _ := range Nodes {
		ordered_nodes = append(ordered_nodes, node)
		likelyhood_score[node] = float64(float64(Nodes[node]) / float64(Wallets[Wallet]) * 100)
	}

	sort.SliceStable(ordered_nodes, func(i, j int) bool {
		return likelyhood_score[ordered_nodes[i]] > likelyhood_score[ordered_nodes[j]]
	})

	var data = make(map[string]map[string]float64)

	for node := range likelyhood_score {

		_, found := data[node]
		if !found {
			data[node] = make(map[string]float64)
		}

		d := data[node]
		d["likelyhood"] = likelyhood_score[node]
		d["minis"] = float64(Minis[node])
		d["finals"] = float64(Finals[node])
		d["orphans"] = float64(Orphans[node])

		data[node] = d
	}

	return ordered_nodes, data
}

func BestGuessMinerNodeHeight(height int64, Wallet string) (string, float64) {
	log_miniblock_mutex.Lock()
	defer log_miniblock_mutex.Unlock()

	var Nodes = make(map[string]int)
	var Total = make(map[string]int)

	for _, block := range FinalBlockLogs {

		if block.Block.Height >= uint64(height) && block.MinerWallet == Wallet {
			Total[block.MinerWallet]++
			Nodes[block.NodeAddress]++
		}
	}

	for _, block := range MiniblockLogs {
		if block.Miniblock.Height >= uint64(height) && block.MinerWallet == Wallet {
			Total[block.MinerWallet]++
			Nodes[block.NodeAddress]++
		}
	}

	var ordered_nodes []string
	for node, _ := range Nodes {
		ordered_nodes = append(ordered_nodes, node)
	}

	sort.SliceStable(ordered_nodes, func(i, j int) bool {
		return Nodes[ordered_nodes[i]] > Nodes[ordered_nodes[j]]
	})

	probability := float64(float64(Nodes[ordered_nodes[0]])/float64(Total[Wallet])) * 100

	return ordered_nodes[0], probability
}

func GetActiveMinersFromHeight(height int64) map[string]map[string]int {
	log_miniblock_mutex.Lock()
	defer log_miniblock_mutex.Unlock()

	var ActiveMiners = make(map[string]map[string]int)

	for _, bl := range FinalBlockLogs {
		if bl.Block.Height >= uint64(height) {
			_, found := ActiveMiners[bl.MinerWallet]
			if !found {
				ActiveMiners[bl.MinerWallet] = make(map[string]int)
			}
			stat, _ := ActiveMiners[bl.MinerWallet]

			stat["finals"]++
			stat["total"]++

			if bl.IsOrphan {
				stat["orphans"]++
			}
			ActiveMiners[bl.MinerWallet] = stat
		}
	}

	for _, mbl := range MiniblockLogs {
		if mbl.Miniblock.Height >= uint64(height) {
			_, found := ActiveMiners[mbl.MinerWallet]
			if !found {
				ActiveMiners[mbl.MinerWallet] = make(map[string]int)
			}
			stat, _ := ActiveMiners[mbl.MinerWallet]

			stat["minis"]++
			stat["total"]++

			if mbl.IsOrphan {
				stat["orphans"]++
			}

			ActiveMiners[mbl.MinerWallet] = stat
		}
	}

	return ActiveMiners
}

func GetActiveNodesFromHeight(height int64) map[string]map[string]int {
	log_miniblock_mutex.Lock()
	defer log_miniblock_mutex.Unlock()

	var ActiveNodes = make(map[string]map[string]int)
	for _, bl := range FinalBlockLogs {
		if bl.Block.Height >= uint64(height) {

			_, found := ActiveNodes[bl.NodeAddress]
			if !found {

				ActiveNodes[bl.NodeAddress] = make(map[string]int)
			}

			stat := ActiveNodes[bl.NodeAddress]
			stat["finals"]++
			stat["total"]++
			if bl.IsOrphan {
				stat["orphans"]++
			}
			ActiveNodes[bl.NodeAddress] = stat
		}
	}

	for _, mbl := range MiniblockLogs {
		if mbl.Miniblock.Height >= uint64(height) {

			_, found := ActiveNodes[mbl.NodeAddress]
			if !found {
				ActiveNodes[mbl.NodeAddress] = make(map[string]int)
			}

			stat := ActiveNodes[mbl.NodeAddress]
			stat["minis"]++
			stat["total"]++
			if mbl.IsOrphan {
				stat["orphans"]++
			}
			ActiveNodes[mbl.NodeAddress] = stat
		}
	}

	return ActiveNodes
}

func GetFinalBlocksFromHeight(height uint64) map[string]FinalBlockLog {

	log_miniblock_mutex.Lock()
	defer log_miniblock_mutex.Unlock()

	var Blocks = make(map[string]FinalBlockLog)

	for hash, block := range FinalBlockLogs {
		if block.Block.Height >= height {
			Blocks[hash] = block
		}
	}

	return Blocks

}

func GetMiniBlocksFromHeight(height uint64) map[string]MiniBlockLog {

	log_miniblock_mutex.Lock()
	defer log_miniblock_mutex.Unlock()

	var Blocks = make(map[string]MiniBlockLog)

	for hash, block := range MiniblockLogs {
		if block.Miniblock.Height >= height {
			Blocks[hash] = block
		}
	}

	return Blocks

}

func LogFinalBlock(bl block.Block, Address string) {

	log_miniblock_mutex.Lock()
	defer log_miniblock_mutex.Unlock()

	BlockHash := fmt.Sprintf("%s", bl.GetHash())
	Address = ParseIPNoError(Address)

	stat, found := FinalBlockLogs[BlockHash]

	if !found {

		if MinerWallet, err1 := rpc.NewAddressFromCompressedKeys(bl.Miner_TX.MinerAddress[:]); err1 == nil {
			stat.MinerWallet = MinerWallet.String()
		}
		stat.Block = bl
		stat.NodeAddress = Address

		FinalBlockLogs[BlockHash] = stat
	}

}

var last_mining_time uint16

func LogMiniblock(mbl block.MiniBlock, Address string) {

	log_miniblock_mutex.Lock()
	defer log_miniblock_mutex.Unlock()

	MiniblockHash := fmt.Sprintf("%s", mbl.GetHash())
	Address = ParseIPNoError(Address)

	stat, found := MiniblockLogs[MiniblockHash]

	if !found {

		MinerWallet := GetMinerAddressFromKeyHash(chain, mbl)
		stat.MinerWallet = MinerWallet
		stat.Miniblock = mbl
		stat.NodeAddress = Address

		MiniblockLogs[MiniblockHash] = stat

	}

}

func LogAccept(Address string) {

	Stats_mutex.Lock()
	defer Stats_mutex.Unlock()

	Address = ParseIPNoError(Address)

	stat := BlockInsertCount[Address]
	stat.Blocks_Accepted++
	BlockInsertCount[Address] = stat

}

func LogReject(Address string) {

	Stats_mutex.Lock()
	defer Stats_mutex.Unlock()

	Address = ParseIPNoError(Address)

	stat := BlockInsertCount[Address]
	stat.Blocks_Rejected++
	BlockInsertCount[Address] = stat

}

func ClearAllStats() {

	go ClearPstat()

	peer_mutex.Lock()
	defer peer_mutex.Unlock()

	for _, p := range peer_map {
		p.FailCount = 0
		p.GoodCount = 0
	}

}

func ClearPstat() {
	Stats_mutex.Lock()
	defer Stats_mutex.Unlock()

	for Address, _ := range Pstat {
		var new_peer_stat PeerStats
		Pstat[Address] = new_peer_stat
	}

	for Address := range BlockInsertCount {
		stat := BlockInsertCount[Address]
		stat.Blocks_Accepted = 0
		stat.Blocks_Rejected = 0
		BlockInsertCount[Address] = stat
	}
}

func PstatCount() (total_peer_sending_error_count int, total_peer_receiving_error_count int, collision_count int) {

	Stats_mutex.Lock()
	defer Stats_mutex.Unlock()

	for _, ps := range Pstat {
		total_peer_sending_error_count += len(ps.Sending_Errors)
		total_peer_receiving_error_count += len(ps.Receiving_Errors)
		collision_count += len(ps.Collision_Errors)
	}

	return total_peer_sending_error_count, total_peer_receiving_error_count, collision_count
}

func ClearPeerStats(Address string) {

	Address = ParseIPNoError(Address)

	Stats_mutex.Lock()
	defer Stats_mutex.Unlock()

	_, x := Pstat[Address]
	if x {
		var new_peer_stat PeerStats
		Pstat[Address] = new_peer_stat
	}

	stat, y := BlockInsertCount[Address]
	if y {
		stat.Blocks_Accepted = 0
		stat.Blocks_Rejected = 0
		BlockInsertCount[Address] = stat
	}
}

func SelfishNodeCounter(Address string, Block_Type string, PeerID uint64, Message string, BlockData []byte) {

	Selfish_mutex.Lock()
	defer Selfish_mutex.Unlock()

	Address = ParseIPNoError(Address)

	// If errors showing connection error, then log this so peer can get cleaned up
	context_deadline := regexp.MustCompile("^context deadline exceeded")
	connection_down := regexp.MustCompile("^connection is shut down")
	closed_pipe := regexp.MustCompile("io: read/write on closed pipe")

	if !connection_down.Match([]byte(Message)) && !closed_pipe.Match([]byte(Message)) && !context_deadline.Match([]byte(Message)) {

		// Check if collision and if it's valid
		//fmt.Errorf("collision %x", mbl.Serialize()), false
		is_collision := regexp.MustCompile("^collision ")
		if is_collision.Match([]byte(Message)) {

			res := strings.TrimPrefix(Message, "collision ")

			if res != fmt.Sprintf("%x", BlockData) {
				logger.Info(fmt.Sprintf("Selfish Node (%s) identified - replied with BAD collision message (%s) vs (%x)", Address, res, BlockData))
			}
		}
		var Error MyBlockReceivingError

		Error.Block_Type = Block_Type
		Error.When = time.Now()
		Error.Error_Message = Message

		logs := SelfishNodeStats[Address]
		logs = append(logs, Error)
		SelfishNodeStats[Address] = logs

	}

}

func GetPeerRBS(Address string) (Collisions uint64, CollisionRate float64, TIPFailCount uint64, TIPFailRate float64) {

	Address = ParseIPNoError(Address)

	Selfish_mutex.Lock()
	defer Selfish_mutex.Unlock()

	is_tip_issue := regexp.MustCompile("^tip could not be expanded")
	is_collision := regexp.MustCompile("^collision ")

	logs, x := SelfishNodeStats[Address]

	Collisions = 0
	TIPFailCount = 0

	if x {
		for _, log := range logs {

			if is_collision.Match([]byte(log.Error_Message)) {
				Collisions++
			}
			if is_tip_issue.Match([]byte(log.Error_Message)) {
				TIPFailCount++
			}
		}
	}

	if globals.BlocksMined < 1 {
		return Collisions, float64(0), TIPFailCount, float64(0)
	}

	CollisionRate = 0
	TIPFailRate = 0

	if Collisions >= 1 {
		CollisionRate = float64((float64(Collisions) / float64(globals.BlocksMined)) * 100)
	}

	if TIPFailCount >= 1 {
		TIPFailRate = float64((float64(TIPFailCount) / float64(globals.BlocksMined)) * 100)
	}

	return Collisions, CollisionRate, TIPFailCount, TIPFailRate
}

func PeerLogConnectionFail(Address string, Block_Type string, PeerID uint64, Message string) {

	Stats_mutex.Lock()
	defer Stats_mutex.Unlock()

	Address = ParseIPNoError(Address)

	peer := Pstat[Address]

	is_collision := regexp.MustCompile("^collision ")
	is_tip_issue := regexp.MustCompile("^tip could not be expanded")

	if is_collision.Match([]byte(Message)) || is_tip_issue.Match([]byte(Message)) {

		stat := peer.Collision_Errors

		var Error BlockCollisionError
		Error.Block_Type = Block_Type
		Error.When = time.Now()
		Error.Error_Message = Message
		Error.Incoming = false
		Error.Peer_ID = PeerID

		stat = append(stat, Error)

		peer.Collision_Errors = stat

		// check collision is genuine

	} else {
		// Log error

		stat := peer.Sending_Errors

		var Error BlockSendingError
		Error.Block_Type = Block_Type
		Error.When = time.Now()
		Error.Error_Message = Message
		Error.Destination_Peer_ID = PeerID

		stat = append(stat, Error)

		peer.Sending_Errors = stat
	}

	context_deadline := regexp.MustCompile("^context deadline exceeded")
	// If errors showing connection error, then log this so peer can get cleaned up
	connection_down := regexp.MustCompile("^connection is shut down")
	closed_pipe := regexp.MustCompile("io: read/write on closed pipe")

	if connection_down.Match([]byte(Message)) || closed_pipe.Match([]byte(Message)) || context_deadline.Match([]byte(Message)) {
		go Peer_SetFail(Address)
	}

	Pstat[Address] = peer
}

func PeerLogReceiveFail(Address string, Block_Type string, PeerID uint64, Message string) {

	Stats_mutex.Lock()
	defer Stats_mutex.Unlock()

	Address = ParseIPNoError(Address)

	peer := Pstat[Address]

	is_collision := regexp.MustCompile("^collision ")
	is_tip_issue := regexp.MustCompile("^tip could not be expanded")

	if is_collision.Match([]byte(Message)) || is_tip_issue.Match([]byte(Message)) {

		stat := peer.Collision_Errors

		var Error BlockCollisionError
		Error.Block_Type = Block_Type
		Error.When = time.Now()
		Error.Error_Message = Message
		Error.Incoming = true
		Error.Peer_ID = PeerID

		stat = append(stat, Error)

		peer.Collision_Errors = stat

	} else {
		// Log error
		stat := peer.Receiving_Errors

		var Error BlockReceivingError
		Error.Block_Type = Block_Type
		Error.When = time.Now()
		Error.Error_Message = Message
		Error.From_Peer_ID = PeerID

		stat = append(stat, Error)

		peer.Receiving_Errors = stat
	}
	Pstat[Address] = peer
}

func GetPeerBTS(Address string) (Accepted uint64, Rejected uint64, Total uint64, SuccessRate float64) {

	Stats_mutex.Lock()
	defer Stats_mutex.Unlock()

	Address = ParseIPNoError(Address)

	stat, ps := BlockInsertCount[Address]
	if ps {

		total := float64(stat.Blocks_Accepted + stat.Blocks_Rejected)
		SuccessRate = (float64(stat.Blocks_Accepted) / total) * 100

		return stat.Blocks_Accepted, stat.Blocks_Rejected, (stat.Blocks_Accepted + stat.Blocks_Rejected), SuccessRate
	}

	return Accepted, Rejected, Total, SuccessRate
}

func ClearPeerLogsCron() {

	Stats_mutex.Lock()
	defer Stats_mutex.Unlock()

	cleared_counter := 0
	for peer, stat := range Pstat {

		var Sending_Errors []BlockSendingError
		var Receiving_Errors []BlockReceivingError
		var Collision_Errors []BlockCollisionError

		for _, log := range stat.Sending_Errors {
			if log.When.Unix()+config.RunningConfig.ErrorLogExpirySeconds > time.Now().Unix() {
				Sending_Errors = append(Sending_Errors, log)
			} else {
				cleared_counter++
			}
		}

		for _, log := range stat.Receiving_Errors {
			if log.When.Unix()+config.RunningConfig.ErrorLogExpirySeconds > time.Now().Unix() {
				Receiving_Errors = append(Receiving_Errors, log)
			} else {
				cleared_counter++
			}
		}

		for _, log := range stat.Collision_Errors {
			if log.When.Unix()+config.RunningConfig.ErrorLogExpirySeconds > time.Now().Unix() {
				Collision_Errors = append(Collision_Errors, log)
			} else {
				cleared_counter++
			}
		}

		stat.Sending_Errors = Sending_Errors
		stat.Receiving_Errors = Receiving_Errors
		stat.Collision_Errors = Collision_Errors

		if len(stat.Sending_Errors) == 0 && len(stat.Receiving_Errors) == 0 && len(stat.Collision_Errors) == 0 {
			delete(Pstat, peer)
		} else {
			Pstat[peer] = stat
		}
	}

	logger.V(2).Info(fmt.Sprintf("Cleared (%d) peer logs", cleared_counter))
}
