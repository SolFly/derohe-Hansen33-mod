package rpc

import (
	"flag"
	"fmt"
	"net/http"
	"sort"
	"time"

	"bytes"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"encoding/hex"
	"encoding/json"
	"encoding/pem"
	"math/big"
	"net"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/deroproject/derohe/block"
	"github.com/deroproject/derohe/config"
	"github.com/deroproject/derohe/globals"
	"github.com/deroproject/derohe/rpc"
	"github.com/deroproject/graviton"
	"github.com/go-logr/logr"
	"github.com/lesismal/llib/std/crypto/tls"
	"github.com/lesismal/nbio"
	"github.com/lesismal/nbio/logging"
	"github.com/lesismal/nbio/nbhttp"
	"github.com/lesismal/nbio/nbhttp/websocket"
)

// this file implements the non-blocking job streamer
// only job is to stream jobs to thousands of workers, if any is successful,accept and report back

var memPool = sync.Pool{
	New: func() interface{} {
		return make([]byte, 16*1024)
	},
}

var logger_getwork logr.Logger
var (
	svr   *nbhttp.Server
	print = flag.Bool("print", false, "stdout output of echoed data")
)

type user_session struct {
	blocks        uint64
	miniblocks    uint64
	rejected      uint64
	lasterr       string
	address       rpc.Address
	valid_address bool
	address_sum   [32]byte
}

type banned struct {
	fail_count uint8
	timestamp  time.Time
}

var client_list_mutex sync.Mutex
var client_list = map[*websocket.Conn]*user_session{}
var ban_list = make(map[string]banned)

var miners_count int

// this will track miniblock rate,
var mini_found_time []int64 // this array contains a epoch timestamp in int64
var rate_lock sync.Mutex

//this function will return wrong result if too wide time glitches happen to system clock
func Counter(seconds int64) (r int) { // we need atleast 1 mini to find a rate
	rate_lock.Lock()
	defer rate_lock.Unlock()
	length := len(mini_found_time)
	if length > 0 {
		start_point := time.Now().Unix() - seconds
		i := sort.Search(length, func(i int) bool { return mini_found_time[i] >= start_point })
		if i < len(mini_found_time) {
			r = length - i
		}
	}
	return // return 0
}

func cleanup() {
	rate_lock.Lock()
	defer rate_lock.Unlock()
	length := len(mini_found_time)
	if length > 0 {
		start_point := time.Now().Unix() - 30*24*3600 // only keep data of last 30 days
		i := sort.Search(length, func(i int) bool { return mini_found_time[i] >= start_point })
		if i > 1000 && i < length {
			mini_found_time = append(mini_found_time[:0], mini_found_time[i:]...) // renew the array
		}
	}
}

// this will calcuate amount of hashrate based on the number of minis
// note this calculation is very crude
// note it will always be lagging, since NW conditions are quite dynamic
// this is  used to roughly estimate your hash rate on this integrator of all miners
// note this is a moving avg
func HashrateEstimatePercent(timeframe int64) float64 {
	return float64(Counter(timeframe)*100) / (float64(timeframe*10) / float64(config.BLOCK_TIME))
}

// note this will be be 0, if you have less than 1/48000 hash power
func HashrateEstimatePercent_1hr() float64 {
	return HashrateEstimatePercent(3600)
}

// note result will be 0, if you have  less than 1/2000 hash power
func HashrateEstimatePercent_1day() float64 {
	return HashrateEstimatePercent(24 * 3600)
}

// note this will be 0, if you have less than 1/(48000*7)
func HashrateEstimatePercent_7day() float64 {
	return HashrateEstimatePercent(7 * 24 * 3600)
}

func CountMiners() int {
	defer cleanup()
	client_list_mutex.Lock()
	defer client_list_mutex.Unlock()
	miners_count = len(client_list)
	return miners_count
}

var CountUniqueMiners int64
var CountMinisAccepted int64 // total accepted which passed Powtest, chain may still ignore them
var CountMinisRejected int64 // total rejected // note we are only counting rejected as those which didnot pass Pow test
var CountMinisOrphaned int64
var CountBlocks int64 //  total blocks found as integrator, note that block can still be a orphan

// total = CountAccepted + CountRejected + CountBlocks(they may be orphan or may not get rewarded)

type inner_miner_stats struct {
	blocks     uint64
	miniblocks uint64
	rejected   uint64
	orphaned   uint64
	address    string
}

var miner_stats_mutex sync.Mutex
var miner_stats = make(map[string]inner_miner_stats)

func GetMinerWallet(ip_address string) string {
	miner_stats_mutex.Lock()
	defer miner_stats_mutex.Unlock()

	wallet := ""

	for miner, stat := range miner_stats {
		if miner == ip_address {
			return stat.address
		}
	}

	return wallet
}

func UpdateMinerStats() {

	client_list_mutex.Lock()
	defer client_list_mutex.Unlock()

	miner_stats_mutex.Lock()
	defer miner_stats_mutex.Unlock()

	var unique_miners = make(map[string]int)

	for conn, sess := range client_list {

		unique_miners[sess.address.String()]++

		// use address with port for uniqueness - could have more than one miner behind same IP but mining to different addresses
		i := miner_stats[conn.RemoteAddr().String()]

		i.blocks = sess.blocks
		i.miniblocks = sess.miniblocks
		i.rejected = sess.rejected

		_, found := block.MyOrphanBlocks[conn.RemoteAddr().String()]
		if found {
			i.orphaned = uint64(len(block.MyOrphanBlocks[conn.RemoteAddr().String()]))
		} else {
			i.orphaned = 0
		}

		i.address = fmt.Sprintf("%s", sess.address)

		miner_stats[conn.RemoteAddr().String()] = i

	}

	CountUniqueMiners = int64(len(unique_miners))

	// reset counter
	CountMinisOrphaned = 0
	for miner := range miner_stats {
		_, found := block.MyOrphanBlocks[miner]
		if found {
			CountMinisOrphaned += int64(len(block.MyOrphanBlocks[miner]))
		}
	}
}

func MinerIsConnected(ip_address string) bool {

	client_list_mutex.Lock()
	defer client_list_mutex.Unlock()

	for conn := range client_list {
		if conn.RemoteAddr().String() == ip_address {
			return true
		}
	}

	return false
}

func ShowMinerInfo(wallet string) {

	fmt.Print("Miner Info\n\n")

	miner_stats_mutex.Lock()
	defer miner_stats_mutex.Unlock()

	count := 0
	for ip_address, stat := range miner_stats {

		if fmt.Sprintf("%s", stat.address) != wallet && ParseIPNoError(wallet) != ParseIPNoError(ip_address) {
			continue
		}

		if count == 0 {
			fmt.Printf("Miner Wallet: %s\n\n", stat.address)
			fmt.Printf("%-32s %-12s %-12s %-12s %-12s %-12s %-12s\n\n", "IP Address", "Connected", "Blocks", "Mini Blocks", "Rejected", "Orphan", "Success Rate")
		}
		count++

		is_connected := "no"
		miners_connected := uint64(0)

		if MinerIsConnected(ip_address) {
			is_connected = "yes"
			miners_connected++
		}

		good_blocks := stat.blocks + stat.miniblocks
		bad_blocks := stat.rejected + stat.orphaned

		success_rate := float64(100)

		if bad_blocks >= 1 {

			success_rate = float64(100 - float64(float64(float64(bad_blocks)/float64(good_blocks)*100)))

		}

		fmt.Printf("%-32s %-12s %-12d %-12d %-12d %-12d %.2f\n", ip_address, is_connected, stat.blocks, stat.miniblocks, stat.rejected, stat.orphaned, success_rate)

	}

	fmt.Print("\n")

}

type miner_counter struct {
	miners           uint64
	miners_connected uint64
	blocks           uint64
	miniblocks       uint64
	rejected         uint64
	orphaned         uint64
	is_connected     string
	address          rpc.Address
}

func ListMiners() {

	var miners = make(map[string]miner_counter)

	// Aggregate miner stats per wallet
	miner_stats_mutex.Lock()
	defer miner_stats_mutex.Unlock()

	for ip_address, stat := range miner_stats {
		i := miners[fmt.Sprintf("%s", stat.address)]

		i.blocks += stat.blocks
		i.miniblocks += stat.miniblocks
		i.rejected += stat.rejected
		i.orphaned += stat.orphaned
		i.miners++

		if MinerIsConnected(ip_address) {
			i.is_connected = "yes"
			i.miners_connected++
		} else if i.is_connected != "yes" {
			i.is_connected = "no"
		}
		miners[fmt.Sprintf("%s", stat.address)] = i
	}

	fmt.Print("Connected Miners\n\n")

	fmt.Printf("%-72s %-10s %-12s %-12s %-12s %-12s %-12s %-12s\n\n", "Wallet", "Connected", "Miners", "Blocks", "Mini Blocks", "Rejected", "Orphan", "Success Rate")

	for wallet, stat := range miners {

		miners_connected_str := fmt.Sprintf("%d", stat.miners)
		if stat.miners != stat.miners_connected {
			miners_connected_str = fmt.Sprintf("%d/%d", stat.miners_connected, stat.miners)
		}

		good_blocks := stat.blocks + stat.miniblocks
		bad_blocks := stat.rejected + stat.orphaned

		success_rate := float64(100)

		if bad_blocks >= 1 {

			success_rate = float64(100 - float64(float64(float64(bad_blocks)/float64(good_blocks)*100)))

		}

		fmt.Printf("%-72s %-10s %-12s %-12d %-12d %-12d %-12d %.2f\n", wallet, stat.is_connected, miners_connected_str, stat.blocks, stat.miniblocks, stat.rejected, stat.orphaned, success_rate)

	}

	fmt.Print("\n")
}

func SendJob() {

	defer globals.Recover(1)

	// get a block template, and then we will fill the address here as optimization
	bl, mbl_main, _, _, err := chain.Create_new_block_template_mining(chain.IntegratorAddress())
	if err != nil {
		return
	}

	prev_hash := ""
	for i := range bl.Tips {
		prev_hash = prev_hash + bl.Tips[i].String()
	}

	diff := chain.Get_Difficulty_At_Tips(bl.Tips)

	if mbl_main.HighDiff {
		diff.Mul(diff, new(big.Int).SetUint64(config.MINIBLOCK_HIGHDIFF))
	}
	client_list_mutex.Lock()
	defer client_list_mutex.Unlock()

	for rk, rv := range client_list {

		go func(k *websocket.Conn, v *user_session) {
			defer globals.Recover(2)
			var buf bytes.Buffer
			encoder := json.NewEncoder(&buf)

			var params rpc.GetBlockTemplate_Result
			params.JobID = fmt.Sprintf("%d.%d.%s", bl.Timestamp, 0, "notified")
			params.Height = bl.Height
			params.Prev_Hash = prev_hash
			params.Difficultyuint64 = diff.Uint64()
			params.Difficulty = diff.String()

			mbl := mbl_main

			if !mbl.Final { //write miners address only if possible
				copy(mbl.KeyHash[:], v.address_sum[:])
			}

			for i := range mbl.Nonce { // give each user different work
				mbl.Nonce[i] = globals.Global_Random.Uint32() // fill with randomness
			}

			if !v.valid_address && !chain.IsAddressHashValid(false, v.address_sum) {
				params.LastError = "unregistered miner or you need to wait 15 mins"
			} else {
				v.valid_address = true
			}
			params.Blockhashing_blob = fmt.Sprintf("%x", mbl.Serialize())
			params.Blocks = v.blocks
			params.MiniBlocks = v.miniblocks
			params.Rejected = v.rejected

			encoder.Encode(params)
			k.SetWriteDeadline(time.Now().Add(100 * time.Millisecond))
			k.WriteMessage(websocket.TextMessage, buf.Bytes())
			buf.Reset()

		}(rk, rv)

	}
	go UpdateMinerStats()

}

func newUpgrader() *websocket.Upgrader {
	u := websocket.NewUpgrader()
	go UpdateMinerStats()

	u.OnMessage(func(c *websocket.Conn, messageType websocket.MessageType, data []byte) {
		// echo
		//c.WriteMessage(messageType, data)

		if messageType != websocket.TextMessage {
			return
		}

		sess := c.Session().(*user_session)
		miner := ParseIPNoError(c.RemoteAddr().String())

		client_list_mutex.Lock()
		defer client_list_mutex.Unlock()

		var p rpc.SubmitBlock_Params

		if err := json.Unmarshal(data, &p); err != nil {

		}

		mbl_block_data_bytes, err := hex.DecodeString(p.MiniBlockhashing_blob)
		if err != nil {
			//logger.Info("Submitting block could not be decoded")
			sess.lasterr = fmt.Sprintf("Submitted block could not be decoded. err: %s", err)
			return
		}

		var tstamp, extra uint64
		fmt.Sscanf(p.JobID, "%d.%d", &tstamp, &extra)

		_, blid, sresult, err := chain.Accept_new_block(tstamp, mbl_block_data_bytes)

		if sresult {

			// Save mini and miner
			var mbl block.MiniBlock

			if err = mbl.Deserialize(mbl_block_data_bytes); err != nil {
				logger.V(1).Error(err, "Error Deserializing newly minted block")
			} else {
				go block.AddBlockToMyCollection(mbl, c.RemoteAddr().String())
			}

			//logger.Infof("Submitted block %s accepted", blid)
			if blid.IsZero() {
				sess.miniblocks++
				atomic.AddInt64(&CountMinisAccepted, 1)

				rate_lock.Lock()
				defer rate_lock.Unlock()
				mini_found_time = append(mini_found_time, time.Now().Unix())

				// Reset fail count in case of valid PoW
				for i, t := range ban_list {
					if miner == i {
						t.fail_count = 0
						ban_list[miner] = t
					}
				}

			} else {
				sess.blocks++
				atomic.AddInt64(&CountBlocks, 1)
			}
		}

		if !sresult || err != nil {
			sess.rejected++
			atomic.AddInt64(&CountMinisRejected, 1)

			rate_lock.Lock()
			defer rate_lock.Unlock()

			// Increase fail count and ban miner in case of 3 invalid PoW's in a row
			i := ban_list[miner]
			i.fail_count++
			if i.fail_count >= 3 {
				i.timestamp = time.Now()
				c.Close()
				delete(client_list, c)
				logger_getwork.Info("Banned miner", "Address", miner, "Info", "Banned")
			}
			ban_list[miner] = i
		}

		go UpdateMinerStats()

	})
	u.OnClose(func(c *websocket.Conn, err error) {
		client_list_mutex.Lock()
		defer client_list_mutex.Unlock()
		delete(client_list, c)

	})

	return u
}

func onWebsocket(w http.ResponseWriter, r *http.Request) {
	if !strings.HasPrefix(r.URL.Path, "/ws/") {
		http.NotFound(w, r)
		return
	}
	address := strings.TrimPrefix(r.URL.Path, "/ws/")

	addr, err := globals.ParseValidateAddress(address)
	if err != nil {
		fmt.Fprintf(w, "err: %s\n", err)
		return
	}
	addr_raw := addr.PublicKey.EncodeCompressed()

	upgrader := newUpgrader()
	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		//panic(err)
		return
	}

	// Check incoming connections if ban still exists
	// Ban is active for 15 minutes
	miner := ParseIPNoError(r.RemoteAddr)
	rate_lock.Lock()

	for i, t := range ban_list {
		if miner == i {
			if time.Now().Sub(t.timestamp) < time.Minute*15 {
				logger_getwork.V(1).Info("Banned miner", "Address", i, "Info", "Ban still active")
				conn.Close()
				break
			} else {
				delete(ban_list, i)
				break
			}
		}
	}
	rate_lock.Unlock()

	wsConn := conn.(*websocket.Conn)

	session := user_session{address: *addr, address_sum: graviton.Sum(addr_raw)}
	wsConn.SetSession(&session)

	client_list_mutex.Lock()
	defer client_list_mutex.Unlock()
	client_list[wsConn] = &session

}

func Getwork_server() {

	var err error

	logger_getwork = globals.Logger.WithName("GETWORK")

	logging.SetLevel(logging.LevelNone) //LevelDebug)//LevelNone)

	tlsConfig := &tls.Config{
		Certificates:       []tls.Certificate{generate_random_tls_cert()},
		InsecureSkipVerify: true,
	}

	mux := &http.ServeMux{}
	mux.HandleFunc("/", onWebsocket) // handle everything

	default_address := fmt.Sprintf("0.0.0.0:%d", globals.Config.GETWORK_Default_Port)

	if _, ok := globals.Arguments["--getwork-bind"]; ok && globals.Arguments["--getwork-bind"] != nil {
		addr, err := net.ResolveTCPAddr("tcp", globals.Arguments["--getwork-bind"].(string))
		if err != nil {
			logger_getwork.Error(err, "--getwork-bind address is invalid")
			return
		} else {
			if addr.Port == 0 {
				logger_getwork.Info("GETWORK server is disabled, No ports will be opened for miners to get work")
				return
			} else {
				default_address = addr.String()
			}
		}
	}

	logger_getwork.Info("GETWORK will listen", "address", default_address)

	svr = nbhttp.NewServer(nbhttp.Config{
		Name:                    "GETWORK",
		Network:                 "tcp",
		AddrsTLS:                []string{default_address},
		TLSConfig:               tlsConfig,
		Handler:                 mux,
		MaxLoad:                 10 * 1024,
		MaxWriteBufferSize:      32 * 1024,
		ReleaseWebsocketPayload: true,
		KeepaliveTime:           240 * time.Hour, // we expects all miners to find a block every 10 days,
		NPoller:                 runtime.NumCPU(),
	})

	svr.OnReadBufferAlloc(func(c *nbio.Conn) []byte {
		return memPool.Get().([]byte)
	})
	svr.OnReadBufferFree(func(c *nbio.Conn, b []byte) {
		memPool.Put(b)
	})

	//globals.Cron.AddFunc("@every 2s", SendJob) // if daemon restart automaticaly send job
	go func() { // try to be as optimized as possible to lower hash wastage
		if config.RunningConfig.GETWorkJobDispatchTime.Milliseconds() < 40 {
			config.RunningConfig.GETWorkJobDispatchTime = 500 * time.Millisecond
		}
		logger_getwork.Info("Job will be dispatched every", "time", config.RunningConfig.GETWorkJobDispatchTime)
		old_mini_count := 0
		old_time := time.Now()
		old_height := int64(0)
		for {
			if miners_count > 0 {
				current_mini_count := chain.MiniBlocks.Count()
				current_height := chain.Get_Height()
				if old_mini_count != current_mini_count || old_height != current_height || time.Now().Sub(old_time) > config.RunningConfig.GETWorkJobDispatchTime {
					old_mini_count = current_mini_count
					old_height = current_height
					SendJob()
					old_time = time.Now()
				}
			} else {

			}
			time.Sleep(10 * time.Millisecond)
		}
	}()

	if err = svr.Start(); err != nil {
		logger_getwork.Error(err, "nbio.Start failed.")
		return
	}
	logger.Info("GETWORK/Websocket server started")
	svr.Wait()
	defer svr.Stop()

}

// generate default tls cert to encrypt everything
// NOTE: this does NOT protect from individual active man-in-the-middle attacks
func generate_random_tls_cert() tls.Certificate {

	/* RSA can do only 500 exchange per second, we need to be faster
	     * reference https://github.com/golang/go/issues/20058
	    key, err := rsa.GenerateKey(rand.Reader, 512) // current using minimum size
	if err != nil {
	    log.Fatal("Private key cannot be created.", err.Error())
	}

	// Generate a pem block with the private key
	keyPem := pem.EncodeToMemory(&pem.Block{
	    Type:  "RSA PRIVATE KEY",
	    Bytes: x509.MarshalPKCS1PrivateKey(key),
	})
	*/
	// EC256 does roughly 20000 exchanges per second
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	b, err := x509.MarshalECPrivateKey(key)
	if err != nil {
		logger.Error(err, "Unable to marshal ECDSA private key")
		panic(err)
	}
	// Generate a pem block with the private key
	keyPem := pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: b})

	tml := x509.Certificate{
		SerialNumber: big.NewInt(int64(time.Now().UnixNano())),

		// TODO do we need to add more parameters to make our certificate more authentic
		// and thwart traffic identification as a mass scale

		// you can add any attr that you need
		NotBefore: time.Now().AddDate(0, -1, 0),
		NotAfter:  time.Now().AddDate(1, 0, 0),
		// you have to generate a different serial number each execution
		/*
		   Subject: pkix.Name{
		       CommonName:   "New Name",
		       Organization: []string{"New Org."},
		   },
		   BasicConstraintsValid: true,   // even basic constraints are not required
		*/
	}
	cert, err := x509.CreateCertificate(rand.Reader, &tml, &tml, &key.PublicKey, key)
	if err != nil {
		logger.Error(err, "Certificate cannot be created.")
		panic(err)
	}

	// Generate a pem block with the certificate
	certPem := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: cert})
	tlsCert, err := tls.X509KeyPair(certPem, keyPem)
	if err != nil {
		logger.Error(err, "Certificate cannot be loaded.")
		panic(err)
	}
	return tlsCert
}

func ParseIP(s string) (string, error) {
	ip, _, err := net.SplitHostPort(s)
	if err == nil {
		return ip, nil
	}

	ip2 := net.ParseIP(s)
	if ip2 == nil {
		return "", fmt.Errorf("invalid IP")
	}

	return ip2.String(), nil
}

func ParseIPNoError(s string) string {
	ip, _ := ParseIP(s)
	return ip
}
