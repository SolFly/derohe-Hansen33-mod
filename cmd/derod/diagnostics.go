package main

import (
	"fmt"
	"io"
	"os"
	"path"
	"runtime"
	"sync/atomic"
	"time"

	"github.com/chzyer/readline"
	"github.com/deroproject/derohe/blockchain"
	"github.com/deroproject/derohe/config"
	"github.com/deroproject/derohe/globals"
	"github.com/deroproject/derohe/p2p"
	"gopkg.in/natefinch/lumberjack.v2"
)

var DiagnosticInterval uint64 = 1

func ToggleDebug(l *readline.Instance, log_level int8) {

	if config.RunningConfig.LogLevel == log_level {
		return
	}

	exename, _ := os.Executable()

	if config.RunningConfig.LogLevel > 0 {
		logger.Info(fmt.Sprint("Disabling DEBUG (some connection might take few seconds)"))
	}

	logger.Info(fmt.Sprintf("Updating log level to (%d) .. ", log_level))

	globals.SetLogLevel(l.Stdout(), &lumberjack.Logger{
		Filename:   path.Base(exename) + "-diags.log",
		MaxSize:    100, // megabytes
		MaxBackups: 2,
	}, (0 - int(log_level)))

	logger = globals.Logger.WithName("derod")

	p2p_logger := globals.Logger.WithName("P2P")
	p2p.SetLogger(&p2p_logger)

	core_logger := globals.Logger.WithName("CORE")
	blockchain.SetLogger(&core_logger)

	logger.V(1).Info("Debug (ENABLED)")

	config.RunningConfig.LogLevel = log_level

}

var boot_timer int64 = globals.StartTime.Unix()

func RunDiagnosticCheckSquence(chain *blockchain.Blockchain, l *readline.Instance) {

	// when - this is check all the time so

	if p2p.Peer_Count() <= 10 && time.Now().Unix()-globals.StartTime.Unix() < 180 {

		if time.Now().Unix()-boot_timer > 10 {
			logger.Info("DERO System is still booting")
			boot_timer = time.Now().Unix()
		}
		return
	}
	if time.Now().Unix() < globals.NextDiagnocticCheck {
		return
	}
	if globals.DiagnocticCheckRunning {
		return
	}
	globals.DiagnocticCheckRunning = true

	var critical_errors []string
	var peer_errors []string

	w := l.Stdout()

	var old_debug_level = config.RunningConfig.LogLevel
	ToggleDebug(l, 0)

	if old_debug_level > 0 {
		time.Sleep(3 * time.Second)
	}

	io.WriteString(w, "\n* Diagnostics Sequence Initiated ... \n\n")
	logger.Info("", "OS", runtime.GOOS, "ARCH", runtime.GOARCH, "GOMAXPROCS", runtime.GOMAXPROCS(0))
	logger.Info("", "Version", config.Version.String())

	time.Sleep(1 * time.Second)

	io.WriteString(w, "\n* Checking Block Chain Status... \n")

	our_height := chain.Get_Height()
	best_height, _ := p2p.Best_Peer_Height()
	tips := chain.Get_TIPS()
	current_blid := tips[0]

	if best_height == 0 {
		io.WriteString(w, fmt.Sprintf("\tError.. Blockchain height not received from Peers... (%d)\n", best_height))
		critical_errors = append(critical_errors, fmt.Sprintf("Blockchain height not received from Peers... We got (%d)", best_height))
		globals.NetworkTurtle = true
	} else {
		io.WriteString(w, fmt.Sprintf("\tOK: Peers returning Blockchain height .. (%d)\n", best_height))
	}

	if our_height > best_height && best_height > 0 {
		time.Sleep(1 * time.Second)
		io.WriteString(w, fmt.Sprintf("\tOK: Looking very good Captain, we're ahead of most of the fleet ..  Fleet (%d) vs Us (%d), we're flying\n", best_height, our_height))
		globals.NetworkTurtle = false
	}
	time.Sleep(1 * time.Second)

	if our_height == 0 {
		io.WriteString(w, fmt.Sprintf("\tError.. Own Blockchain height is (%d) .. Engines still warming up\n", our_height))
		critical_errors = append(critical_errors, fmt.Sprintf("Own Blockchain height is (%d) .. Engines still warming up", our_height))
		globals.NetworkTurtle = true
	} else {
		io.WriteString(w, fmt.Sprintf("\tOK: Our Blockchain height verified .. (%d)\n", our_height))
	}
	time.Sleep(1 * time.Second)

	if best_height == our_height || our_height+1 == best_height || our_height-1 == best_height {
		io.WriteString(w, fmt.Sprintf("\tOK: Blockchain height and node verified .. Height is (%d)\n", our_height))
		globals.NetworkTurtle = false
	} else {
		io.WriteString(w, fmt.Sprintf("\tError.. Node height not in line with the network. Status reads - Network (%d) vs Us (%d)\n", best_height, our_height))
		critical_errors = append(critical_errors, fmt.Sprintf("Node height (%d) not in line with the network (%d)", our_height, best_height))
		globals.NetworkTurtle = true
	}

	// enable debug
	// check if block chain moves
	io.WriteString(w, "\n* Block Chain and Network Scan Initiated ... \n\n")
	time.Sleep(5000 * time.Millisecond)
	io.WriteString(w, "* Activating Debug Sequence ... \n\n")
	time.Sleep(5000 * time.Millisecond)

	ToggleDebug(l, 1)
	var block_chain_start_height = best_height
	var our_chain_start_height = our_height
	var last_blid = current_blid
	for i := 0; i <= 23; i++ {

		time.Sleep(1 * time.Second)

		our_height = chain.Get_Height()
		best_height, _ = p2p.Best_Peer_Height()

		logger.V(1).Info(fmt.Sprintf("Logging Current Stats: Current Peer Height (%d) - Our Height (%d)", best_height, our_height))

		tips = chain.Get_TIPS()
		current_blid = tips[0]

		logger.V(1).Info(fmt.Sprintf("Logging Current blid: (%d)", current_blid))

	}
	ToggleDebug(l, 0)
	time.Sleep(2 * time.Second)

	logger.V(1).Info(fmt.Sprintf("Logging Current Stats: Current Peer Height (%d) - Our Height (%d)", best_height, our_height))
	io.WriteString(w, "\n* Block Chain and Network Scan Finished ... \n")
	io.WriteString(w, "\n* Processing results ... \n")
	time.Sleep(2 * time.Second)

	our_height = chain.Get_Height()
	best_height, _ = p2p.Best_Peer_Height()

	if last_blid == current_blid {
		io.WriteString(w, fmt.Sprintf("\tERROR: Engine is experincing some issues, at tip: %s\n", current_blid.String()))
		critical_errors = append(critical_errors, fmt.Sprintf("Engine issues at tip: %s", current_blid.String()))
		globals.NetworkTurtle = true
	} else {
		io.WriteString(w, fmt.Sprintf("\tOK: Current tip: %s - was: %s\n", current_blid.String(), last_blid.String()))
	}

	if our_height == our_chain_start_height {
		io.WriteString(w, "\tERROR: Our height have not increased, our block chain is stuck!\n")
		critical_errors = append(critical_errors, fmt.Sprintf("Our block chain is stuck at height: %d", our_height))
		globals.NetworkTurtle = true
	} else {
		io.WriteString(w, fmt.Sprintf("\tOK: Our Height %d - was: %d\n", our_height, our_chain_start_height))
	}

	if block_chain_start_height == best_height {
		io.WriteString(w, "\tERROR: We're not getting new height from the network Sir. We have serious issue\n")
		critical_errors = append(critical_errors, fmt.Sprintf("We're not getting new height from the network, stuck at height: %d", our_height))
		globals.NetworkTurtle = true
	} else {
		io.WriteString(w, fmt.Sprintf("\tOK: New Network Height %d - was: %d\n", best_height, block_chain_start_height))
	}

	if block_chain_start_height < best_height && our_chain_start_height < our_height {
		io.WriteString(w, "\tOK: Block Chain is moving as expected ... \n")
		globals.NetworkTurtle = false
	}
	// peer stats
	// loop through connections and look for ones with tag and display them on radar
	io.WriteString(w, "\n* Scanning Stargate Peer(s)  ... \n\n")

	peer_map := p2p.UniqueConnections()
	var friendly_peers []*p2p.Connection
	incoming_count := 0
	outgoing_count := 0
	connected_count := 0
	bad_daemon_count := 0
	good_daemon_count := 0
	bad_height_count := 0
	good_height_count := 0
	peer_count := 0
	peer_whitelist := p2p.Peer_Count_Whitelist()

	for _, peer := range peer_map {

		peer_count++
		Address := p2p.ParseIPNoError(peer.Addr.String())

		if peer.Incoming {
			incoming_count++
		} else {
			outgoing_count++
		}

		if time.Duration(atomic.LoadInt64(&peer.Latency)).Round(time.Millisecond) > config.RunningConfig.PeerLatencyThreshold {
			critical_errors = append(critical_errors, fmt.Sprintf("Peer: %s - Latency: %s", Address, time.Duration(atomic.LoadInt64(&peer.Latency)).Round(time.Millisecond)))
		}
		//check if latency is good or bad - and repotr

		if p2p.IsAddressConnected(p2p.ParseIPNoError(peer.Addr.String())) {
			connected_count++
		}
		if our_height >= peer.Height {
			good_height_count++

		} else {
			bad_height_count++
		}

		AcceptedCount, RejectedCount, TotalErrors, SuccessRate := p2p.GetPeerBTS(Address)
		// check if this is a bad actor
		if TotalErrors >= 100 && SuccessRate <= float64(config.RunningConfig.BlockRejectThreshold) {
			peer_errors = append(peer_errors, fmt.Sprintf("Peer: %s - Is a suspecious actor, BTS: %d Accepted / %d Rejected - %.2f%%", Address, AcceptedCount, RejectedCount, SuccessRate))
			critical_errors = append(critical_errors, fmt.Sprintf("Peer: %s - Is a potential bad actor (BTS: %.2f%%), investigate and consider ban", Address, SuccessRate))
		}

		if len(peer.Tag) >= 1 {
			friendly_peers = append(friendly_peers, peer)
		}
		// topo_height := chain.Load_TOPO_HEIGHT()

		// report if daemon version is different than ours
		if config.Version.String() != peer.DaemonVersion {
			bad_daemon_count++
		} else {
			good_daemon_count++
		}

	}
	time.Sleep(1 * time.Second)

	if incoming_count == 0 {
		peer_errors = append(peer_errors, "We have no incoming Peers - this makes communication very difficult!")
		critical_errors = append(critical_errors, fmt.Sprintf("ACTION: No Incoming Peers - Make sure Port %d is allowing incoming UDP traffic", p2p.P2P_Port))
	}

	if peer_count < int(p2p.Min_Peers) {
		peer_errors = append(peer_errors, fmt.Sprintf("We have less peer(s) than we want, we have %d and we would ideally like to have %d", peer_count, p2p.Min_Peers))
		critical_errors = append(critical_errors, fmt.Sprintf("We have %d of %d peer(s) requested - try increase minimum peers", peer_count, p2p.Min_Peers))
	}

	if peer_count == 0 {
		peer_errors = append(peer_errors, "We seem to be have NO peers at all.")
		critical_errors = append(critical_errors, "We have NO (0) Peer(s) connected, check network settings")
	}

	// io.WriteString(w, "\n* Analysing Peer Scan Results ... \n\n")
	// time.Sleep(1 * time.Second)

	if len(peer_errors) >= 1 {
		io.WriteString(w, "\n\tPeering issues found during our diagnostic checks\n\n")
		for i := 0; i < len(peer_errors); i++ {
			time.Sleep(100 * time.Millisecond)
			io.WriteString(w, fmt.Sprintf("\t%-10s %-10s\n", (fmt.Sprintf("[%d]", i)), peer_errors[i]))
		}

	}
	io.WriteString(w, "\n")
	if len(friendly_peers) >= 1 {
		io.WriteString(w, fmt.Sprintf("\tWe have found %d peers displaying call signs, these might be friendlies.\n", len(friendly_peers)))
		io.WriteString(w, fmt.Sprintf("\t\t%-22s %-22s", "Remote Addr", "Node Tag"))

		for _, friend := range friendly_peers {
			io.WriteString(w, fmt.Sprintf("\t\t%-22s %-22s\n", p2p.ParseIPNoError(friend.Addr.String()), friend.Tag))

		}
	} else {
		io.WriteString(w, "\tWe have found no friendly nodes, calling a few friends.\n")
		go p2p.ConnecToNode("213.171.208.37:18089") // dero-node.mysrv.cloud
		go p2p.ConnecToNode("74.208.211.24:11011")  // dero-node-us.mysrv.cloud
		go p2p.ConnecToNode("77.68.102.85:11011")   // dero-node.mysrv.cloud
	}
	// minis in memory count
	// Mining stags

	// Checking if engines are still in turtle mode
	if peer_whitelist >= 10 && best_height > 0 && best_height >= our_height {
		globals.NetworkTurtle = false
	}

	// set next diagnostic check in 1 hour
	globals.NextDiagnocticCheck = time.Now().Unix() + config.RunningConfig.DiagnosticCheckDelay

	// mempool_tx_count := len(chain.Mempool.Mempool_List_TX())
	// regpool_tx_count := len(chain.Regpool.Regpool_List_TX())

	if globals.NetworkTurtle {
		critical_errors = append(critical_errors, "Node in turtle mode")
		time.Sleep(100 * time.Millisecond)
		if peer_count < int(p2p.Min_Peers) {
			io.WriteString(w, fmt.Sprintf("\tWe have less peer(s) than we want, we have %d and we would ideally like to have %d\n", peer_count, p2p.Min_Peers))
			critical_errors = append(critical_errors, fmt.Sprintf("We have %d of %d peers requested\n", peer_count, p2p.Min_Peers))
		} else {
			critical_errors = append(critical_errors, "Engines are locked in turtle mode, check CPU, DISK and Network Load")

		}
	}

	total_peer_sending_error_count, total_peer_receiving_error_count, collision_count := p2p.PstatCount()

	io.WriteString(w, "\n\n*** Captain, our diagnostic report ****\n\n")

	hostname, _ := os.Hostname()
	io.WriteString(w, fmt.Sprintf("\tHostname: %s - Uptime: %s\n", hostname, time.Now().Sub(globals.StartTime).Round(time.Second).String()))
	io.WriteString(w, fmt.Sprintf("\tUptime Since: %s\n", globals.StartTime.Format(time.RFC1123)))

	io.WriteString(w, "\n\tPeer Summary:\n")
	io.WriteString(w, fmt.Sprintf("\t\tOur Peer ID: %d\n", p2p.GetPeerID()))
	io.WriteString(w, fmt.Sprintf("\t\tLogged Peer Error(s): %d Sending - %d Receiving - Collisions: %d\n", total_peer_sending_error_count, total_peer_receiving_error_count, collision_count))
	io.WriteString(w, fmt.Sprintf("\t\tCurrent Peer Count %d (Wanted: %d) - %d is currently connected\n", peer_count, p2p.Min_Peers, connected_count))
	io.WriteString(w, fmt.Sprintf("\t\t%d Peer(s) is running same verion as us, and %d is running a different version\n", good_daemon_count, bad_daemon_count))
	io.WriteString(w, fmt.Sprintf("\t\t%d Peer(s) is same height as us, and %d is a at different height\n", good_height_count, bad_height_count))

	if len(critical_errors) >= 1 {
		io.WriteString(w, "\n\tDiagnostic results:\n\n")
		for i := 0; i < len(critical_errors); i++ {
			time.Sleep(100 * time.Millisecond)
			io.WriteString(w, fmt.Sprintf("\t%-10s %-10s\n", (fmt.Sprintf("[%d]", i)), critical_errors[i]))
		}

	}

	if !globals.NetworkTurtle {

		if len(peer_errors) >= 1 || len(critical_errors) >= 1 {
			io.WriteString(w, fmt.Sprintf("\n\tDespite some (%d) peering issues...\n", len(peer_errors)))
			time.Sleep(500 * time.Millisecond)
			if len(critical_errors) >= 1 {
				io.WriteString(w, fmt.Sprintf("\tSome (%d) other issues...\n", len(critical_errors)))
				time.Sleep(500 * time.Millisecond)
			}
		}
		io.WriteString(w, "\n\tNode seems to be performing well. We're good to go.\n")

	} else {
		io.WriteString(w, "\n\nOur team is continuing work on the whole 'Turtle Mode' issue and will run further diagnostics shortly\n\n")
	}
	io.WriteString(w, "\n")
	globals.DiagnocticCheckRunning = false
	ToggleDebug(l, old_debug_level)
}
