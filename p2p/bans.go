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
	"net"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/deroproject/derohe/globals"
)

//import "sort"

//import "encoding/binary"
//import "container/list"

//import log "github.com/sirupsen/logrus"

//import "github.com/deroproject/derosuite/crypto"

// This structure is used to do book keeping for the peer list and keeps other DATA related to peer
// all peers are servers, means they have exposed a port for connections
// all peers are identified by their endpoint tcp address
// all clients are identified by ther peer id ( however ip-address is used to control amount )
// the following daemon commands interact with the list
// bans := print current ban list
// ban address  time  // ban this address for specific time, if time not provided , default ban for 10 minutes
// unban address
// enableban address  // by default all addresses are bannable
// disableban address  // this address will never be banned

var ban_map = map[string]uint64{}     // keeps ban maps
var permban_map = map[string]uint64{} // keeps ban maps

var ban_mutex sync.Mutex

// loads peers list from disk
func load_ban_list() {
	go ban_clean_up_goroutine() // start routine to clean up ban list
	defer ban_clean_up()        // cleanup list after loading it
	ban_mutex.Lock()
	defer ban_mutex.Unlock()

	ban_file := filepath.Join(globals.GetDataDirectory(), "ban_list.json")

	if _, err := os.Stat(ban_file); errors.Is(err, os.ErrNotExist) {
		return // since file doesn't exist , we cannot load it
	}
	file, err := os.Open(ban_file)
	if err != nil {
		logger.Error(err, "opening ban data file")
	} else {
		defer file.Close()
		decoder := json.NewDecoder(file)
		err = decoder.Decode(&ban_map)
		if err != nil {
			logger.Error(err, "Error unmarshalling ban data")
		} else { // successfully unmarshalled data
			logger.V(1).Info("Successfully loaded bans from file", "ban_count", (len(ban_map)))
		}
	}
}

// loads peers list from disk
func load_permban_list() {

	ban_file := filepath.Join(globals.GetDataDirectory(), "permban_list.json")

	if _, err := os.Stat(ban_file); errors.Is(err, os.ErrNotExist) {
		return // since file doesn't exist , we cannot load it
	}
	file, err := os.Open(ban_file)
	if err != nil {
		logger.Error(err, "opening ban data file")
	} else {
		defer file.Close()
		decoder := json.NewDecoder(file)
		err = decoder.Decode(&permban_map)
		if err != nil {
			logger.Error(err, "Error unmarshalling ban data")
		} else { // successfully unmarshalled data
			logger.V(1).Info("Successfully loaded perm bans from file", "ban_count", (len(permban_map)))
		}
	}
}

//save ban list to disk
func save_ban_list() {

	ban_clean_up() // cleanup before saving
	ban_mutex.Lock()
	defer ban_mutex.Unlock()

	ban_file := filepath.Join(globals.GetDataDirectory(), "ban_list.json")
	file, err := os.Create(ban_file)
	if err != nil {
		logger.Error(err, "creating ban data file")
	} else {
		defer file.Close()
		encoder := json.NewEncoder(file)
		encoder.SetIndent("", "\t")
		err = encoder.Encode(&ban_map)
		if err != nil {
			logger.Error(err, "Error unmarshalling ban data")
		} else { // successfully unmarshalled data
			logger.V(1).Info("Successfully saved bans to file", "count", (len(ban_map)))
		}
	}
}

//save ban list to disk
func save_permban_list() {

	ban_file := filepath.Join(globals.GetDataDirectory(), "permban_list.json")
	file, err := os.Create(ban_file)
	if err != nil {
		logger.Error(err, "creating perm ban data file")
	} else {
		defer file.Close()
		encoder := json.NewEncoder(file)
		encoder.SetIndent("", "\t")
		err = encoder.Encode(&permban_map)
		if err != nil {
			logger.Error(err, "Error unmarshalling ban data")
		} else { // successfully unmarshalled data
			logger.V(1).Info("Successfully saved perm bans to file", "count", (len(permban_map)))
		}
	}
}

// clean up ban list every 20 seconds
func ban_clean_up_goroutine() {

	delay := time.NewTicker(20 * time.Second)
	for {
		select {
		case <-Exit_Event:
			return
		case <-delay.C:
		}
		ban_clean_up()
	}

}

/*  used for manual simulation once to track a hard to find bug
func init() {
	load_ban_list()
	Ban_Address("188.191.128.64/24",600)

	IsAddressInBanList("35.185.240.198")
}
*/

// clean up by discarding entries which are  in the past
func ban_clean_up() {
	ban_mutex.Lock()
	defer ban_mutex.Unlock()

	current_time := uint64(time.Now().UTC().Unix())
	for k, v := range ban_map {
		if v < current_time {
			delete(ban_map, k)
		}
	}

}

// convert address to subnet form
// address is an IP address or ipnet in string form
func ParseAddress(address string) (ipnet *net.IPNet, result string, err error) {
	var ip net.IP
	ip, ipnet, err = net.ParseCIDR(address)
	if err != nil { // other check whether the the IP is an IP
		ip = net.ParseIP(address)
		if ip == nil {
			err = fmt.Errorf("IP address could not be parsed")
			return
		} else {
			err = nil
			result = ip.String()
		}

	} else {
		result = ipnet.String()
	}
	return
}

// check whether an IP is in the map already
//  we should loop and search in subnets also
// TODO make it fast, however we are not expecting millions of bans, so this may be okay for time being
func IsAddressInBanList(address string) bool {
	ban_mutex.Lock()
	defer ban_mutex.Unlock()

	// any i which cannot be banned should never be banned
	// this list contains any seed nodes/exclusive nodes/proirity nodes
	// these are never banned
	for i := range nonbanlist {
		if address == nonbanlist[i] {
			return true
		}
	}

	// if it's a subnet or direct ip, do instant check
	if _, ok := ban_map[address]; ok {
		return true
	}

	// if it's a subnet or direct ip, do instant check
	if _, ok := permban_map[address]; ok {
		return true
	}

	ip := net.ParseIP(address) // user provided a valid ip parse and check subnet
	if ip != nil {

		// parse and check the subnets
		for k, _ := range ban_map {
			ipnet, _, err := ParseAddress(k)

			//  fmt.Printf("parsing address %s err %s  checking ip %s",k,err,address)

			// fmt.Printf("\nipnet %+v", ipnet)
			if ip.String() == k || (err == nil && ipnet != nil && ipnet.Contains(ip)) {
				return true
			}

		}
	}
	return false
}

// ban a peer for specific time
// manual bans are always placed
// address can be an address or a subnet

func PermBan_Address(address string) (err error) {
	ban_mutex.Lock()
	defer ban_mutex.Unlock()

	// make sure we are not banning seed nodes/exclusive node/priority nodes on command line
	_, address, err = ParseAddress(address)
	if err != nil {
		return
	}

	logger.Info(fmt.Sprintf("Address: %s - Added to Perm-Ban List", address))
	permban_map[address] = uint64(time.Now().UTC().Unix())
	go save_permban_list()
	go Ban_Address(address, 3600)
	return
}

func Ban_Address(address string, ban_seconds uint64) (err error) {
	ban_mutex.Lock()
	defer ban_mutex.Unlock()

	// make sure we are not banning seed nodes/exclusive node/priority nodes on command line
	_, address, err = ParseAddress(address)
	if err != nil {
		return
	}

	if IsTrustedIP(address) {
		logger.Info(fmt.Sprintf("Address (%s) is trusted list or a seed node not banning", address))
		return
	}

	//logger.Warnf("%s banned for %d secs", address, ban_seconds)
	ban_map[address] = uint64(time.Now().UTC().Unix()) + ban_seconds

	connection_map.Range(func(k, value interface{}) bool {
		v := value.(*Connection)
		if ParseIPNoError(address) == ParseIPNoError(v.Addr.String()) {
			v.Client.Close()
			v.Conn.Close()
			connection_map.Delete(Address(v))
			return false
		}
		return true
	})

	return
}

/*
/// ban a peer only if it can be banned
func Peer_Ban_Internal(address string, ban_seconds uint64) (err error){
    p := GetPeerInList(address)
    if p == nil {
     return fmt.Errorf("Peer \"%s\" not found in list")
    }
    p.Lock()
    defer p.Unlock()
    if p.NeverBlacklist == false {
        p.BlacklistBefore = uint64(time.Now().Unix()) + ban_seconds
    }
}
*/

// unban a peer for specific time
func UnBan_Address(address string) (err error) {
	if !IsAddressInBanList(address) {
		return fmt.Errorf("Address \"%s\" not found in ban list", address)
	}
	ban_mutex.Lock()
	defer ban_mutex.Unlock()

	logger.Info("unbanned", "address", address)

	_, bm := ban_map[address]
	if bm {
		delete(ban_map, address)
	}

	// remove from autoban
	_, pb := permban_map[address]
	if pb {
		delete(permban_map, address)
	}

	return
}

/*
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

// prints all the connection info to screen
func BanList_Print() {
	ban_clean_up() // clean up before printing
	ban_mutex.Lock()
	defer ban_mutex.Unlock()

	fmt.Printf("%-22s %-22s %-8s\n", "Addr", "Seconds to unban", "Perm Ban")
	for k, v := range ban_map {

		is_permban := "no"
		_, ab := permban_map[k]
		if ab {
			is_permban = "yes"
		}

		fmt.Printf("%-22s %-22d %-8s\n", k, v-uint64(time.Now().UTC().Unix()), is_permban)
	}

	fmt.Printf("Ban List contains: %d - Perm Ban List: %d\n", len(ban_map), len(permban_map))
}

// this function return peer count which have successful handshake
func Ban_Count() (Count uint64) {
	ban_mutex.Lock()
	defer ban_mutex.Unlock()
	return uint64(len(ban_map))
}

func Ban_Above_Height(height int64) {

	for _, c := range UniqueConnections() {

		if c.Height > int64(height) {
			logger.Info(fmt.Sprintf("Banning Peer: %s - Height: %d", c.Addr.String(), c.Height))
			go Ban_Address(ParseIPNoError(c.Addr.String()), 3600)

		}

	}

}
