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

package p2p

/* this file implements the peer manager, keeping a list of peers which can be tried for connection etc
 *
 */
import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/deroproject/derohe/config"
	"github.com/deroproject/derohe/globals"
	"github.com/go-logr/logr"
)

//import "encoding/binary"
//import "container/list"

//import log "github.com/sirupsen/logrus"

//import "github.com/deroproject/derosuite/crypto"

// This structure is used to do book keeping for the peer list and keeps other DATA related to peer
// all peers are servers, means they have exposed a port for connections
// all peers are identified by their endpoint tcp address
// all clients are identified by ther peer id ( however ip-address is used to control amount )
// the following daemon commands interact with the list
// peer_list := print the peer list
// ban address  time  // ban this address for spcific time
// unban address
// enableban address  // by default all addresses are bannable
// disableban address  // this address will never be banned
type Peer struct {
	Address string `json:"address"` // pairs in the ip:port or dns:port, basically  endpoint
	ID      uint64 `json:"peerid"`  // peer id
	Miner   bool   `json:"miner"`   // miner
	//NeverBlacklist    bool    // this address will never be blacklisted
	LastConnected   uint64 `json:"lastconnected"`   // epoch time when it was connected , 0 if never connected
	FailCount       uint64 `json:"failcount"`       // how many times have we failed  (tcp errors)
	ConnectAfter    uint64 `json:"connectafter"`    // we should connect when the following timestamp passes
	BlacklistBefore uint64 `json:"blacklistbefore"` // peer blacklisted till epoch , priority nodes are never blacklisted, 0 if not blacklist
	GoodCount       uint64 `json:"goodcount"`       // how many times peer has been shared with us
	Version         int    `json:"version"`         // version 1 is original C daemon peer, version 2 is golang p2p version
	Whitelist       bool   `json:"whitelist"`
	sync.Mutex
}

var peer_map = map[string]*Peer{}
var peer_mutex sync.Mutex

// loads peers list from disk
func load_peer_list() {
	defer clean_up()
	peer_mutex.Lock()
	defer peer_mutex.Unlock()

	peer_file := filepath.Join(globals.GetDataDirectory(), "peers.json")
	if _, err := os.Stat(peer_file); errors.Is(err, os.ErrNotExist) {
		return // since file doesn't exist , we cannot load it
	}
	file, err := os.Open(peer_file)
	if err != nil {
		logger.Error(err, "opening peer file")
	} else {
		defer file.Close()
		decoder := json.NewDecoder(file)
		err = decoder.Decode(&peer_map)
		if err != nil {
			logger.Error(err, "Error unmarshalling peer data")
		} else { // successfully unmarshalled data
			logger.V(1).Info("Successfully loaded peers from file", "peer_count", (len(peer_map)))
		}
	}

}

// this function return peer count which have successful handshake
func Peer_Count_Whitelist() (Count uint64) {
	peer_mutex.Lock()
	defer peer_mutex.Unlock()
	for _, p := range peer_map {
		if p.Whitelist { // only display white listed peer
			// whitelisted = "yes"
			Count++
		}
	}

	return
}

//save peer list to disk
func save_peer_list() {

	clean_up()
	peer_mutex.Lock()
	defer peer_mutex.Unlock()

	peer_file := filepath.Join(globals.GetDataDirectory(), "peers.json")
	file, err := os.Create(peer_file)
	if err != nil {
		logger.Error(err, "saving peer file")
	} else {
		defer file.Close()
		encoder := json.NewEncoder(file)
		encoder.SetIndent("", "\t")
		err = encoder.Encode(&peer_map)
		if err != nil {
			logger.Error(err, "Error marshalling peer data")
		} else { // successfully unmarshalled data
			logger.V(1).Info("Successfully saved peers to file", "peer_count", (len(peer_map)))
		}
	}
}

// clean up by discarding entries which are too much into future
func clean_up() {
	peer_mutex.Lock()
	defer peer_mutex.Unlock()
	for k, v := range peer_map {
		if IsAddressConnected(ParseIPNoError(v.Address)) {
			v.FailCount = 0
			continue
		}
		if v.FailCount >= 8 { // roughly 8 tries before we discard the peer
			delete(peer_map, k)
		}
		if v.LastConnected == 0 { // if never connected, purge the peer
			delete(peer_map, k)
		}

		if uint64(time.Now().UTC().Unix()) > (v.LastConnected + 3600) { // purge all peers which were not connected in
			delete(peer_map, k)
		}
	}
}

// check whether an IP is in the map already
func IsPeerInList(address string) bool {
	peer_mutex.Lock()
	defer peer_mutex.Unlock()

	if _, ok := peer_map[ParseIPNoError(address)]; ok {
		return true
	}
	return false
}
func GetPeerInList(address string) *Peer {
	peer_mutex.Lock()
	defer peer_mutex.Unlock()

	if v, ok := peer_map[ParseIPNoError(address)]; ok {
		return v
	}
	return nil
}

// add connection to  map
func Peer_Add(p *Peer) {
	clean_up()
	peer_mutex.Lock()
	defer peer_mutex.Unlock()

	if p.ID == GetPeerID() { // if peer is self do not connect
		// logger.Infof("Peer is ourselves, discard")
		return

	}

	// trusted only if enabled
	if config.RunningConfig.OnlyTrusted {

		// First make sure we remove all untrusted connections
		for _, conn := range UniqueConnections() {
			if !IsTrustedIP(conn.Addr.String()) {
				logger.V(1).Info(fmt.Sprintf("Disconnecting: %s", conn.Addr.String()))
				conn.Client.Close()
				conn.Conn.Close()
				Connection_Delete(conn)
			}
		}

		// Check if new peer is trusted before adding it
		if !IsTrustedIP(p.Address) {
			logger.V(1).Info(fmt.Sprintf("Trusted Only Mode: %s is not a trusted node - ignored", p.Address))
			return
		}
	}

	if _, ok := permban_map[ParseIPNoError(p.Address)]; ok {
		logger.V(1).Info(fmt.Sprintf("Peer (%s) on Perm Ban List - Blocked", p.Address))
		Ban_Address(ParseIPNoError(p.Address), 3600)
		return
	}

	if v, ok := peer_map[ParseIPNoError(p.Address)]; ok {
		v.Lock()
		// logger.Infof("Peer already in list adding good count")
		v.GoodCount++
		v.Unlock()
	} else {
		// logger.Infof("Peer adding to list")
		peer_map[ParseIPNoError(p.Address)] = p
	}

}

func SetLogger(newlogger *logr.Logger) {

	peer_mutex.Lock()
	defer peer_mutex.Unlock()

	connection_map.Range(func(k, value interface{}) bool {
		c := value.(*Connection)

		logger = *newlogger
		c.logger = logger.WithName("incoming").WithName(c.Addr.String())

		return true
	})

}

func PrintBlockErrors() {

	Stats_mutex.Lock()
	defer Stats_mutex.Unlock()

	fmt.Printf("\nPeer Block Distribution - Errors Log (last %s)\n", time.Duration(config.RunningConfig.ErrorLogExpirySeconds*int64(time.Second)).Round(time.Second))

	fmt.Printf("\n%-16s %-32s %-8s %-22s\n", "Remote Addr", "Errors (Receiving / Sending)", "BTS", "Lastest Error")
	error_count := 0
	peer_count := 0
	for Address, stat := range Pstat {

		var success_rate float64 = 100
		_, ps := BlockInsertCount[Address]
		if ps {
			total := (BlockInsertCount[Address].Blocks_Accepted + BlockInsertCount[Address].Blocks_Rejected)
			success_rate = float64(float64(float64(BlockInsertCount[Address].Blocks_Accepted) / float64(total) * 100))
		}

		errors_text := fmt.Sprintf("%d/%d Collisions: %d", len(stat.Receiving_Errors), len(stat.Sending_Errors), len(stat.Collision_Errors))

		if len(stat.Sending_Errors) == 0 && len(stat.Receiving_Errors) == 0 {
			continue
		}

		var latest_error time.Time
		if len(stat.Sending_Errors) >= 1 {
			latest_error = stat.Sending_Errors[len(stat.Sending_Errors)-1].When
		}

		if len(stat.Receiving_Errors) >= 1 {
			if stat.Receiving_Errors[len(stat.Receiving_Errors)-1].When.Unix() > latest_error.Unix() {
				latest_error = stat.Receiving_Errors[len(stat.Receiving_Errors)-1].When
			}
		}
		fmt.Printf("%-16s %-32s %-8.2f %-10s\n", Address, errors_text, success_rate, latest_error.Format(time.RFC1123))

		peer_count++
		error_count += len(stat.Sending_Errors) + len(stat.Receiving_Errors)
	}

	fmt.Printf("\nLogged %d error(s) for %d peer(s)\n", error_count, len(Pstat))
	fmt.Print("Type: peer_error <IP>       - to see connection errors\n\n")

}

func PrintPeerErrors(Address string) {

	Address = ParseIPNoError(Address)

	fmt.Printf("\nPeer Block Distribution - Errors Log (last %s)\n", time.Duration(config.RunningConfig.ErrorLogExpirySeconds*int64(time.Second)).Round(time.Second))

	if len(Address) <= 0 {
		return
	}

	AcceptedCount, RejectedCount, _, SuccessRate := GetPeerBTS(Address)

	Stats_mutex.Lock()
	defer Stats_mutex.Unlock()

	stat, x := Pstat[Address]
	if x {
		if len(stat.Collision_Errors) >= 1 {
			fmt.Print("\nCollision(s):\n")
			for _, error := range stat.Collision_Errors {
				fmt.Printf("*  %-32s %-32s %-32s\n", error.Block_Type, error.When.Format(time.RFC1123), error.Error_Message)
			}
		}

		if len(stat.Receiving_Errors) >= 1 {
			fmt.Print("\nReceiving:\n")
			for _, error := range stat.Receiving_Errors {
				fmt.Printf("*  %-32s %-32s %-32s\n", error.Block_Type, error.When.Format(time.RFC1123), error.Error_Message)
			}
		}

		if len(stat.Sending_Errors) >= 1 {
			fmt.Print("\nSending:\n")
			for _, error := range stat.Sending_Errors {
				fmt.Printf("*  %-32s %-32s %-32s\n", error.Block_Type, error.When.Format(time.RFC1123), error.Error_Message)
			}
		}

		fmt.Printf("\nLogged %d error(s) for IN (%d) - OUT (%d)\n", (len(stat.Sending_Errors) + len(stat.Receiving_Errors)), len(stat.Receiving_Errors), len(stat.Sending_Errors))
		fmt.Printf("Block Transmission Success Rate: %d Accepted / %d Rejected - %.2f%%\n\n", AcceptedCount, RejectedCount, SuccessRate)
	}

}

func Show_Selfish_Peers() {

	fmt.Printf("\nSelfish Peers - Errors Log\n")

	Selfish_mutex.Lock()
	defer Selfish_mutex.Unlock()

	fmt.Printf("%-32s %-22s %-14s %-14s %-14s\n", "Address", "BTS - A/R", "Collision Rate", "Tip Fail Rate", "Total Acceptance Rate")

	for Address, Logs := range SelfishNodeStats {

		is_tip_issue := regexp.MustCompile("^tip could not be expanded")
		is_collision := regexp.MustCompile("^collision ")

		CollisionRate := float64(0)
		TIPFailRate := float64(0)

		Collisions := 0
		TIPFailCount := 0

		for _, log := range Logs {
			if is_collision.Match([]byte(log.Error_Message)) {
				Collisions++
			}
			if is_tip_issue.Match([]byte(log.Error_Message)) {
				TIPFailCount++
			}
		}

		TARate := float64(0)
		TotalAcceptance := float64(Collisions + TIPFailCount)
		if globals.BlocksMined >= 1 {
			if Collisions >= 1 {
				CollisionRate = float64((float64(Collisions) / float64(globals.BlocksMined)) * 100)
			}

			if TIPFailCount >= 1 {
				TIPFailRate = float64((float64(TIPFailCount) / float64(globals.BlocksMined)) * 100)
			}

			if TotalAcceptance >= 1 {
				TARate = float64((TotalAcceptance / float64(globals.BlocksMined)) * 100)
			}

		}

		CollisionText := fmt.Sprintf("%d (%.2f%%)", Collisions, CollisionRate)
		TIPText := fmt.Sprintf("%d (%.2f%%)", TIPFailCount, TIPFailRate)

		AcceptedCount, RejectedCount, _, SuccessRate := GetPeerBTS(Address)
		BTS := fmt.Sprintf("%.2f%% - %d/%d", SuccessRate, AcceptedCount, RejectedCount)

		for _, error := range Logs {

			if is_collision.Match([]byte(error.Error_Message)) || is_tip_issue.Match([]byte(error.Error_Message)) {
				continue
			}
			fmt.Printf("\t%-32s %-32s %-32s\n", error.Block_Type, error.When.Format(time.RFC1123), error.Error_Message)
		}

		ta_out := fmt.Sprintf("%.2f%%", TARate)
		fmt.Printf("%-32s %-22s %-14s %-14s %-14s\n", Address, BTS, CollisionText, TIPText, ta_out)

	}

}

func Print_Peer_Info(Address string) {

	peer_mutex.Lock()
	defer peer_mutex.Unlock()

	Address = ParseIPNoError(Address)

	if len(Address) <= 0 {
		fmt.Printf("usage: peer_info <ip address>\n")
		return
	}

	fmt.Printf("Peer Information Dashboard\n")

	fmt.Printf("%-22s %-23s %-8s %-22s %-12s %-12s %-8s %-14s\n", "Peer ID", "Version", "Height", "Connected", "Port", "Direction", "Latency", "Tag")
	for _, c := range UniqueConnections() {
		if ParseIPNoError(c.Addr.String()) == Address {

			direction := "OUT"
			if c.Incoming {
				direction = "IN"
			}

			state := "PENDING"
			if atomic.LoadUint32(&c.State) == IDLE {
				state = "IDLE"
			} else if atomic.LoadUint32(&c.State) == ACTIVE {
				state = "ACTIVE"
			}

			is_connected := "no"
			if IsAddressConnected(Address) {
				is_connected = fmt.Sprintf("%s (%s)", time.Now().Sub(c.Created).Round(time.Millisecond).String(), state)
			}

			version := c.DaemonVersion
			if len(version) > 20 {
				version = version[:20]
			}

			fmt.Printf("%-22d %-23s %-8d %-22s %-12d %-12s %-8s %-14s\n", c.Peer_ID, version, c.Height, is_connected, c.Port,
				direction, time.Duration(atomic.LoadInt64(&c.Latency)).Round(time.Millisecond).String(), c.Tag)

		}
	}
	fmt.Printf("\n")

	AcceptedCount, RejectedCount, _, SuccessRate := GetPeerBTS(Address)
	fmt.Printf("Block Transmission Success Rate: %d Accepted / %d Rejected - %.2f%%\n\n", AcceptedCount, RejectedCount, SuccessRate)

	Stats_mutex.Lock()
	defer Stats_mutex.Unlock()

	peer := Pstat[Address]
	var latest_sent_error time.Time
	if len(peer.Sending_Errors) >= 1 {
		latest_sent_error = peer.Sending_Errors[len(peer.Sending_Errors)-1].When
	}

	var latest_recv_error time.Time
	if len(peer.Receiving_Errors) >= 1 {
		latest_recv_error = peer.Sending_Errors[len(peer.Receiving_Errors)-1].When
	}

	fmt.Printf("Error Log:\n\t%-20s %-8d Last Error: %s\n\t%-20s %-8dLast Error: %s\n", "Sending Error(s)", len(peer.Sending_Errors), latest_sent_error.Format(time.RFC1123), "Receiving Error(s)", len(peer.Receiving_Errors), latest_recv_error.Format(time.RFC1123))

	fmt.Printf("\nLogged %d error(s) - IN (%d) - OUT (%d)\n", (len(peer.Sending_Errors) + len(peer.Receiving_Errors)), len(peer.Receiving_Errors), len(peer.Sending_Errors))

}

func Peer_Whitelist_Counts() (Count uint64) {
	peer_mutex.Lock()
	defer peer_mutex.Unlock()
	var count = 0
	for _, v := range peer_map {
		if v.Whitelist { // only display white listed peer
			count++
		}
	}
	return uint64(count)
}

func DisconnectAddress(address string) {
	p := GetPeerInList(ParseIPNoError(address))
	if p == nil {
		return
	}

	logger.Info(fmt.Sprintf("Disconnecting Peer: %s", address))
	for _, conn := range UniqueConnections() {
		if ParseIPNoError(conn.Addr.String()) == ParseIPNoError(address) {
			logger.V(1).Info(fmt.Sprintf("Disconnecting: %s", conn.Addr.String()))
			go Connection_Delete(conn)
		}
	}

	go Peer_Delete(p)

}

// a peer marked as fail, will only be connected  based on exponential back-off based on powers of 2
func Peer_SetFail(address string) {
	p := GetPeerInList(ParseIPNoError(address))
	if p == nil {
		return
	}
	peer_mutex.Lock()
	defer peer_mutex.Unlock()

	p.FailCount++ //  increase fail count, and mark for delayed connect

	p.ConnectAfter = uint64(time.Now().UTC().Unix()) + 1<<(p.FailCount-1)
}

// set peer as successfully connected
// we will only distribute peers which have been successfully connected by us
func Peer_SetSuccess(address string) {
	//logger.Infof("Setting peer as success")
	p := GetPeerInList(ParseIPNoError(address))
	if p == nil {
		return
	}
	peer_mutex.Lock()
	defer peer_mutex.Unlock()
	p.FailCount = 0 //  fail count is zero again
	p.ConnectAfter = 0
	p.Whitelist = true
	p.LastConnected = uint64(time.Now().UTC().Unix()) // set time when last connected

	// logger.Infof("Setting peer as white listed")
}

/*
 //TODO do we need a functionality so some peers are never banned
func Peer_DisableBan(address string) (err error){
    p := GetPeerInList(address)
    if p == nil {
     return fmt.Errorf("Peer \"%s\" not found in list")
    }
    p.Lock()
    defer p.Unlock()
    p.NeverBlacklist = true
}

func Peer_EnableBan(address string) (err error){
    p := GetPeerInList(address)
    if p == nil {
     return fmt.Errorf("Peer \"%s\" not found in list")
    }
    p.Lock()
    defer p.Unlock()
    p.NeverBlacklist = false
}
*/

// add connection to  map
func Peer_Delete(p *Peer) {
	peer_mutex.Lock()
	defer peer_mutex.Unlock()
	delete(peer_map, ParseIPNoError(p.Address))
}

// prints all the connection info to screen
func PeerList_Print(limit int64) {
	peer_mutex.Lock()
	defer peer_mutex.Unlock()
	fmt.Printf("Peer List\n")
	// fmt.Printf("%-20s %-22s %-23s %-8s %-22s %-12s %-10s %-8s %-8s %-8s %-8s %-14s\n", "Remote Addr", "Peer ID", "Version", "Height", "Connected", "Direction", "Latency", "IN", "OUT", "Good", "Fail", "Tag")
	fmt.Printf("%-20s %-12s %-22s %-23s %-8s %-22s %-12s %-10s %-8s %-22s %-14s\n", "Remote Addr", "Port", "Peer ID", "Version", "Height", "Connected", "Direction", "Latency", "Good", "Block Success Rate", "Tag")

	active_peers := 0
	pending_peers := 0
	error_peers := uint64(0)
	greycount := 0
	whitelistcount := 0

	var count = int64(0)
	for _, peer := range peer_map {

		connection_map.Range(func(k, value interface{}) bool {
			c := value.(*Connection)

			Address := ParseIPNoError(c.Addr.String())

			if ParseIPNoError(peer.Address) != Address {
				return true
			}

			error_peers += peer.FailCount
			if peer.Whitelist { // only display white listed peer
				// whitelisted = "yes"
				whitelistcount++
			} else {
				greycount++
				return true
			}

			count++
			if count > limit {
				return true
			}

			direction := "OUT"
			if c.Incoming {
				direction = "IN"
			}

			state := "PENDING"
			if atomic.LoadUint32(&c.State) == IDLE {
				pending_peers++
			}
			if atomic.LoadUint32(&c.State) == IDLE {
				state = "IDLE"
			} else if atomic.LoadUint32(&c.State) == ACTIVE {
				state = "ACTIVE"
				active_peers++
			}

			is_connected := "no"
			if IsAddressConnected(Address) {
				is_connected = fmt.Sprintf("%s (%s)", time.Now().Sub(c.Created).Round(time.Millisecond).String(), state)
			}

			version := c.DaemonVersion
			if len(version) > 20 {
				version = version[:20]
			}

			var success_rate float64 = 100
			_, bi := BlockInsertCount[Address]
			if bi {
				total := (BlockInsertCount[Address].Blocks_Accepted + BlockInsertCount[Address].Blocks_Rejected)
				success_rate = float64(float64(float64(BlockInsertCount[Address].Blocks_Accepted) / float64(total) * 100))
			}

			// fmt.Printf("%-20s %-22d %-23s %-8d %-22s %-12s %-10s %-8s %-8s %-8d %-8d %-14s\n", ParseIPNoError(peer.Address), c.Peer_ID, version, c.Height, is_connected,
			// direction, time.Duration(atomic.LoadInt64(&c.Latency)).Round(time.Millisecond).String(), humanize.Bytes(atomic.LoadUint64(&c.BytesIn)),
			// humanize.Bytes(atomic.LoadUint64(&c.BytesOut)), peer.GoodCount, peer.FailCount, c.Tag)

			fmt.Printf("%-20s %-12d %-22d %-23s %-8d %-22s %-12s %-10s %-8d %-22.2f %-14s\n", ParseIPNoError(peer.Address), c.Port, c.Peer_ID, version, c.Height, is_connected,
				direction, time.Duration(atomic.LoadInt64(&c.Latency)).Round(time.Millisecond).String(), peer.GoodCount,
				success_rate, c.Tag)

			return true
		})

	}

	fmt.Printf("\nWhitelist size %d\n", whitelistcount)
	fmt.Printf("Greylist size %d\n", greycount)
	fmt.Printf("Total: %d (Showing Max: %d) - Active: %d - Pending: %d - Error: %d\n", count, limit, active_peers, pending_peers, error_peers)

}

// this function return peer count which are in our list
func Peer_Counts() (Count uint64) {
	peer_mutex.Lock()
	defer peer_mutex.Unlock()
	return uint64(len(peer_map))
}

// this function finds a possible peer to connect to keeping blacklist and already existing connections into picture
// it must not be already connected using outgoing connection
// we do allow loops such as both  incoming/outgoing simultaneously
// this will return atmost 1 address, empty address if peer list is empty
func find_peer_to_connect(version int) *Peer {
	defer clean_up()
	peer_mutex.Lock()
	defer peer_mutex.Unlock()

	// first search the whitelisted ones
	for _, v := range peer_map {
		if uint64(time.Now().Unix()) > v.BlacklistBefore && //  if ip is blacklisted skip it
			uint64(time.Now().Unix()) > v.ConnectAfter &&
			!IsAddressConnected(ParseIPNoError(v.Address)) && v.Whitelist && !IsAddressInBanList(ParseIPNoError(v.Address)) {
			v.ConnectAfter = uint64(time.Now().UTC().Unix()) + 10 // minimum 10 secs gap
			return v
		}
	}
	// if we donot have any white listed, choose from the greylist
	for _, v := range peer_map {
		if !IsAddressConnected(ParseIPNoError(v.Address)) && !IsAddressInBanList(ParseIPNoError(v.Address)) && uint64(time.Now().Unix()) > v.ConnectAfter {
			v.ConnectAfter = uint64(time.Now().UTC().Unix()) + 10 // minimum 10 secs gap
			return v
		}
	}

	return nil // if no peer found, return nil
}

// return white listed peer list
// for use in handshake
func get_peer_list() (peers []Peer_Info) {
	peer_mutex.Lock()
	defer peer_mutex.Unlock()

	for _, v := range peer_map { // trim the white list
		if v.Whitelist && !IsAddressConnected(ParseIPNoError(v.Address)) {
			delete(peer_map, ParseIPNoError(v.Address))
		}
	}

	for _, v := range peer_map {
		if v.Whitelist {
			peers = append(peers, Peer_Info{Addr: v.Address})
		}
	}
	return
}

func get_peer_list_specific(addr string) (peers []Peer_Info) {
	plist := get_peer_list()
	sort.SliceStable(plist, func(i, j int) bool { return plist[i].Addr < plist[j].Addr })

	if len(plist) <= int(Min_Peers) {
		peers = plist
	} else {
		index := sort.Search(len(plist), func(i int) bool { return plist[i].Addr < addr })
		for i := range plist {
			peers = append(peers, plist[(i+index)%len(plist)])
			if len(peers) >= int(Min_Peers) {
				break
			}
		}
	}
	return peers
}
