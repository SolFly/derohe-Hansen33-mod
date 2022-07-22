### Welcome to the DEROHE - Hansen33 Mod

If you don't alredy know DERO - Check out [Dero git repo](https://github.com/deroproject/derohe)

* This is a Weaponized version of Dero Daemon, this is NOT the official release of DERO.
* Please use with caution, as with all weapons you may or may not hurt yourself or others using this.
* If all your minis get lost or your computer blows up, it's no my fault, you installed this.


* This modded DERO version is running and providing live stats for on the nodes below
  * Europe Node - https://dero-node.mysrv.cloud
  * North America Node - https://dero-node-us.mysrv.cloud 
  * South Ammerica Node - https://dero-node-sa.mysrv.cloud 

### Developers

 * @Hansen33
   * Address: dero1qy07h9mk6xxf2k4x0ymdpezvksjy0talskhpvqmat3xk3d9wczg5jqqvwl0sn
 * @arodd
   * Address: dero1qyss8jkqfkvp4vlxfx3l7f9rr7m55ff3q633ag8xemzv0el90m2j2qqy0te2c
 * @mmarcel
   * Address: dero1qydkj6dznyk5njmzr96hjcr5uj74anqqqv90mg39mtzm4d2dcpwsqqqk6zvve

### Changes from official release includes

 More Options, switches and buttons 
 * Miner (--text-mode) - no interactiveness, just mining and output update every 60 sec
 * Active Miner Metrics - Node Guessing
 * Miner Hashrate, Tagging and Orphan to Miner Reporting**
 * Live mining stats including orphan counters.
 * Wallet Socks Proxy Support and TOR Onion Support**
 `  --daemon-address vpilhs5wew52w75igez5fye2c572lccuo7l5emyxxvo53darwec6x7yd.onion:10102`
 * BlocksIn/BlocksOut count in syncinfo - repurposed unused official counters
 * AddressToName - command to look up names inside derod (using HarkerK code)
 * Allow more than 31 incoming peers (will use max_peer option as limit)
 * Miners list and mining performance stats
 * Autoban of bad miners
 * Orphan and mined blocks metrics
 * Node mining performance metrics (more RPC calls)
 * Change Runtime Config - P2P Turbo, BW_FACTOR, Min Peers and more
 * Change debug level during runtime
 * Diagnostic Check and Troubleshooting Tips - Identify bad actors
 * More Peer Info
 * Logging of Peer Errors
 * Permanent Ban IP
 * Permanent Ban List Persistency - Save to file
 * Save and Display Uptime
 * More mining stats
 * Improved Peer List
 * Auto saving bans.json and peers.json
 * More Peers - Removed internal limitations
 * Whitelist incoming connections option
 * Turtle Mode Indicator - If node is running optimal
 * Uptime Stats
 * Some Mining Performance Stats
 * Running Config + System Tuning
 * Updates to Peer Management and Error Logging ( peer_list + peer_errors )
 * Wallet Support for https:// and wss:// when accessing remote wallet
 * Defaulting to use https://dero-node.mysrv.cloud/ as remote node
 * Default integrator address set to dero1qy07h9mk6xxf2k4x0ymdpezvksjy0talskhpvqmat3xk3d9wczg5jqqvwl0sn
 * FIX: Report seconds not mili seconds in simulator
 * Threshold to highlight Peers with Block Transmission Success Rate below x
 * Threshold to highlight Peers latency below x threshold
 * Clear peers stats, individual or all
 * Trusted Mode -Trusted Peers List / Only Connect To Trusted Peers
 * Quick Connect to Hansen Nodes 
 * ban_above_height - ban all peers over a specific height
 * connect_to_peer <ip:port> (Initialise new connection to peer)
 * Quick Connect to Seed Nodes 

### Screen Shots

#### Mining and Miners Stats

![Miners](https://dero-node.mysrv.cloud/images/miner_stats.png)
![Mined Blocks](https://dero-node.mysrv.cloud/images/mined_blocks.png)

#### More Commands

![More Options](https://dero-node.mysrv.cloud/images/more-options.png)
![Running Config](https://dero-node.mysrv.cloud/images/running-config.png)

#### Diagnostics

![Diagnostics](https://dero-node.mysrv.cloud/images/diagnostics.png)
![Diagnostic Report](https://dero-node.mysrv.cloud/images/diagnostic_report.png)

#### Peering Stuff

![Trusted Mode](https://dero-node.mysrv.cloud/images/trusted_mode.png)
![Peer Errors](https://dero-node.mysrv.cloud/images/peer_errors.png)
![Peer Info](https://dero-node.mysrv.cloud/images/peer_info.png)
![Peer List](https://dero-node.mysrv.cloud/images/peer_list.png)

### TODO

 * More Automation
 * More of the same
 * Send feature requests to hansen33#2541 on Discord



