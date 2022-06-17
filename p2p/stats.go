package p2p

import (
	"fmt"
	"regexp"
	"sync"
	"time"
)

type BlockInsertCounter struct {
	Blocks_Accepted uint64
	Blocks_Rejected uint64
}

type BlockSendingError struct {
	Block_ID            string
	Block_Type          string
	When                time.Time
	Error_Message       string
	Destination_Peer_ID uint64
	Address             string
}

type BlockReceivingError struct {
	Block_ID      string
	Block_Type    string
	When          time.Time
	Error_Message string
	From_Peer_ID  uint64
	Address       string
}

type BlockCollisionError struct {
	Block_ID      string
	Block_Type    string
	When          time.Time
	Error_Message string
	Peer_ID       uint64
	Address       string
	Incoming      bool
}

type PeerStats struct {
	Peer_ID          uint64
	Address          string
	Sending_Errors   []*BlockSendingError
	Receiving_Errors []*BlockReceivingError
	Collision_Errors []*BlockCollisionError
}

var Pstat = map[string]*PeerStats{}

var Stats_mutex sync.Mutex

var BlockInsertCount = map[string]*BlockInsertCounter{}

func LogAccept(Address string) {

	Stats_mutex.Lock()
	defer Stats_mutex.Unlock()

	Address = ParseIPNoError(Address)

	_, stat := BlockInsertCount[Address]
	if !stat {
		var NewMetric BlockInsertCounter
		NewMetric.Blocks_Accepted++
		BlockInsertCount[Address] = &NewMetric
	} else {
		BlockInsertCount[Address].Blocks_Accepted++
	}

}

func LogReject(Address string) {

	Stats_mutex.Lock()
	defer Stats_mutex.Unlock()

	Address = ParseIPNoError(Address)

	_, stat := BlockInsertCount[Address]
	if !stat {
		var NewMetric BlockInsertCounter
		NewMetric.Blocks_Rejected++
		BlockInsertCount[Address] = &NewMetric
	} else {
		BlockInsertCount[Address].Blocks_Rejected++

	}

}

func ClearAllStats() {

	peer_mutex.Lock()
	defer peer_mutex.Unlock()

	for _, p := range peer_map {
		p.FailCount = 0
		p.GoodCount = 0
	}

	Stats_mutex.Lock()
	defer Stats_mutex.Unlock()

	var newPstat = map[string]*PeerStats{}
	Pstat = newPstat

	for _, counter := range BlockInsertCount {
		counter.Blocks_Accepted = 0
		counter.Blocks_Rejected = 0
	}
}

func ClearPeerStats(Address string) {

	peer_mutex.Lock()
	defer peer_mutex.Unlock()

	Address = ParseIPNoError(Address)

	for _, p := range peer_map {
		if Address == ParseIPNoError(p.Address) {
			p.FailCount = 0
			p.GoodCount = 0
		}
	}

	Stats_mutex.Lock()
	defer Stats_mutex.Unlock()

	for _, p := range Pstat {
		if Address == p.Address {
			var peer_stats PeerStats
			peer_stats.Address = Address
			Pstat[Address] = &peer_stats
		}
	}

	for key, p := range BlockInsertCount {
		if Address == key {
			p.Blocks_Accepted = 0
			p.Blocks_Rejected = 0
		}
	}
}

func PeerLogConnectionFail(Address string, Block_Type string, Block_ID string, PeerID uint64, Message string) {

	Stats_mutex.Lock()
	defer Stats_mutex.Unlock()

	Address = ParseIPNoError(Address)
	// Create stats object if not already exists
	// Create stats object if not already exists
	_, stat_exists := Pstat[Address]
	if !stat_exists {
		var peer_stats PeerStats
		peer_stats.Peer_ID = PeerID
		peer_stats.Address = Address
		Pstat[Address] = &peer_stats
	}

	is_collision := regexp.MustCompile("^collision ")
	is_tip_issue := regexp.MustCompile("^tip could not be expanded")

	if is_collision.Match([]byte(Message)) || is_tip_issue.Match([]byte(Message)) {
		var CollisionError BlockCollisionError
		CollisionError.Block_ID = Block_ID
		CollisionError.Block_Type = Block_Type
		CollisionError.Address = Address
		CollisionError.When = time.Now()
		CollisionError.Error_Message = Message
		CollisionError.Incoming = false
		Pstat[Address].Collision_Errors = append(Pstat[Address].Collision_Errors, &CollisionError)

	} else {
		// Log error
		var ConnectionError BlockSendingError
		ConnectionError.Block_ID = Block_ID
		ConnectionError.Block_Type = Block_Type
		ConnectionError.Address = Address
		ConnectionError.When = time.Now()
		ConnectionError.Error_Message = Message
		ConnectionError.Destination_Peer_ID = PeerID
		Pstat[Address].Sending_Errors = append(Pstat[Address].Sending_Errors, &ConnectionError)
	}

	go Peer_SetFail(Address)
	go logger.V(2).Info(fmt.Sprintf("Error (%s) - Logged for Connection: %s", Message, Address))

}

func PeerLogReceiveFail(Address string, Block_Type string, Block_ID string, PeerID uint64, Message string) {

	Stats_mutex.Lock()
	defer Stats_mutex.Unlock()

	Address = ParseIPNoError(Address)
	// Create stats object if not already exists
	// Create stats object if not already exists
	_, stat_exists := Pstat[Address]
	if !stat_exists {
		var peer_stats PeerStats
		peer_stats.Peer_ID = PeerID
		peer_stats.Address = Address
		Pstat[Address] = &peer_stats
	}

	is_collision := regexp.MustCompile("^collision ")
	is_tip_issue := regexp.MustCompile("^tip could not be expanded")

	if is_collision.Match([]byte(Message)) || is_tip_issue.Match([]byte(Message)) {
		var CollisionError BlockCollisionError

		CollisionError.Block_ID = Block_ID
		CollisionError.Block_Type = Block_Type
		CollisionError.Address = Address
		CollisionError.When = time.Now()
		CollisionError.Error_Message = Message
		CollisionError.Incoming = true
		Pstat[Address].Collision_Errors = append(Pstat[Address].Collision_Errors, &CollisionError)

	} else {
		// Log error
		var ConnectionError BlockReceivingError

		ConnectionError.Block_ID = Block_ID
		ConnectionError.Block_Type = Block_Type
		ConnectionError.Address = Address
		ConnectionError.When = time.Now()
		ConnectionError.Error_Message = Message
		ConnectionError.From_Peer_ID = PeerID
		Pstat[Address].Receiving_Errors = append(Pstat[Address].Receiving_Errors, &ConnectionError)
	}
	go Peer_SetFail(Address)
	go logger.V(2).Info(fmt.Sprintf("Error (%s) - Logged for Connection: %s", Message, Address))

}
