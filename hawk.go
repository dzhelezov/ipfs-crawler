package main

import (
	"context"
	"crypto/rand"
	"fmt"
	mrand "math/rand"
	"os"
	"os/signal"
	"sync"
	"time"

	"github.com/ipfs/go-datastore"
	badger "github.com/ipfs/go-ds-badger"
	b58 "github.com/mr-tron/base58/base58"

	"github.com/libp2p/go-libp2p"
	"github.com/libp2p/go-libp2p-core/host"
	"github.com/libp2p/go-libp2p-core/network"
	"github.com/libp2p/go-libp2p-core/peer"

	dht "github.com/libp2p/go-libp2p-kad-dht"
	kbucket "github.com/libp2p/go-libp2p-kbucket"
	"github.com/libp2p/go-libp2p/p2p/protocol/identify"
	"github.com/multiformats/go-multiaddr"
	mh "github.com/multiformats/go-multihash"
	"github.com/opentracing/opentracing-go/log"

	_ "github.com/multiformats/go-multiaddr-dns"
)

const WORKERS = 10

type NodeStat struct {
	seen      time.Time
	connected time.Time
	// other info can be taken from the peerstore
}

type Crawler struct {
	host host.Host
	dht  *dht.IpfsDHT
	ds   datastore.Batching
	id   *identify.IDService

	ctx     context.Context
	cancel  context.CancelFunc
	wg      sync.WaitGroup
	limiter <-chan time.Time

	qlog    *os.File
	connlog *os.File
}

type notifee Crawler

var _ network.Notifiee = (*Crawler)(nil)
var mu sync.Mutex
var nodes map[peer.ID]*NodeStat = make(map[peer.ID]*NodeStat)

func NewCrawler() *Crawler {
	ctx, cancel := context.WithCancel(context.Background())

	c := &Crawler{
		ctx:     ctx,
		cancel:  cancel,
		limiter: time.Tick(5000 * time.Millisecond),
	}

	qlog, err := os.OpenFile("random_queries.log", os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0600)
	if err != nil {
		fmt.Println("Can't open the query log file")
		panic(err)
	}
	c.qlog = qlog

	connlog, err := os.OpenFile("conn.log", os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0600)
	if err != nil {
		fmt.Println("Can't open the connection log file")
		panic(err)
	}

	c.connlog = connlog
	c.initHost()
	return c
}

func (c *Crawler) initHost() {
	var err error

	c.host, err = libp2p.New(c.ctx)
	if err != nil {
		panic(err)
	}

	c.ds, err = badger.NewDatastore("dht.db", nil)
	if err != nil {
		panic(err)
	}

	c.dht = dht.NewDHTClient(c.ctx, c.host, c.ds)

	for _, a := range dht.DefaultBootstrapPeers[4:] {
		pi, err := peer.AddrInfoFromP2pAddr(a)
		if err != nil {
			panic(err)
		}
		ctx, cancel := context.WithTimeout(c.ctx, 5*time.Second)
		if err = c.host.Connect(ctx, *pi); err != nil {
			fmt.Printf("skipping over bootstrap peer: %s\n", pi.ID.Pretty())
		}
		cancel()
	}

	c.id = identify.NewIDService(c.ctx, c.host)
}

func (c *Crawler) close() {

	fmt.Println("Shutting down the crawler")
	c.cancel()
	// wait until all the workers finish the queries
	c.wg.Wait()

	if err := c.qlog.Close(); err != nil {
		fmt.Printf("error while closing the query log: %v\n", err)
	}
	if err := c.connlog.Close(); err != nil {
		fmt.Printf("error while closing the connection log: %v\n", err)
	}
	if err := c.ds.Close(); err != nil {
		fmt.Printf("error while shutting down: %v\n", err)
	}
	if err := c.host.Close(); err != nil {
		fmt.Printf("error closing connection from host: %v\n", err)
	}
}

func (c *Crawler) start() {
	fmt.Printf("Starting a new crawler with %d workers\n", WORKERS)

	for i := 0; i < WORKERS; i++ {
		c.wg.Add(1)
		go c.worker()
	}

	c.host.Network().Notify(c)
}

func LogNewPeer(id peer.ID) *NodeStat {
	mu.Lock()
	defer mu.Unlock()

	stats, ok := nodes[id]
	if !ok {
		fmt.Printf("Got new peer %v\n", id.Pretty())

		nodes[id] = &NodeStat{
			seen: time.Now(),
		}
	} else {
		stats.seen = time.Now()
	}
	return nodes[id]
}

func commonPrefix(key string, id peer.ID) int {
	id1 := kbucket.ConvertKey(key)
	id2 := kbucket.ConvertPeerID(id)

	return kbucket.CommonPrefixLen(id1, id2)
}

func (c *Crawler) worker() {
	defer c.wg.Done()
	for {
		select {
		case <-c.ctx.Done():
			fmt.Printf("The crawler is done, stopping the worker\n")
			return
		default:
			<-c.limiter
			randomKey, logKey := getRandomKey()
			fmt.Printf("Querying a random key\n")
			qctx, qcancel := context.WithTimeout(c.ctx, 120*time.Second)
			peers, err := c.dht.GetClosestPeers(qctx, randomKey)
			if err != nil {
				log.Error(err)
				qcancel()
			}

			done := false
			clenmax := 0
			for !done {
				select {
				case pid, ok := <-peers:
					if !ok {
						done = true
						break
					}
					clen := commonPrefix(randomKey, pid)
					fmt.Fprintf(c.qlog, "%d,%s,%s,%d,%d\n", time.Now().Unix(), pid.Pretty(), logKey, clen, (1 << clen))

					if clen > clenmax {
						fmt.Printf("Found common prefix of length %d, estimated node count %d\n", clen, (1 << clen))
						clenmax = clen
					}

				case <-qctx.Done():
					done = true
				}
			}

			qcancel()
			// make a random delay between queries
			time.Sleep(time.Duration(1000+mrand.Intn(500)) * time.Millisecond)
		}
	}
}

func getRandomKey() (string, string) {
	buf := make([]byte, 32)
	rand.Read(buf)
	o, err := mh.Encode(buf, mh.SHA2_256)
	if err != nil {
		panic(err)
	}
	return string(o), b58.Encode(o)
}

func report() {

	for {
		<-time.After(10 * time.Second)
		// an unsynced call is safe here as we just log approx size
		fmt.Printf("====== scraped %d peers in total ====== \n", len(nodes))
		// more stats can be logged here
	}
}

func (n *Crawler) Connected(net network.Network, conn network.Conn) {
	go func() {
		n.wg.Add(1)
		defer n.wg.Done()

		select {
		case <-n.ctx.Done():
			return
		default:	
			p := conn.RemotePeer()
			n.id.IdentifyConn(conn)
			<-n.id.IdentifyWait(conn)

			stats := LogNewPeer(p)
			stats.connected = time.Now()
			fmt.Fprintf(n.connlog, "%d,%s\n", time.Now().Unix(), p.Pretty())
		}	
	}()
}

func (*Crawler) Listen(network.Network, multiaddr.Multiaddr)      {}
func (*Crawler) ListenClose(network.Network, multiaddr.Multiaddr) {}
func (*Crawler) Disconnected(network.Network, network.Conn)       {}
func (*Crawler) OpenedStream(network.Network, network.Stream)     {}
func (*Crawler) ClosedStream(network.Network, network.Stream)     {}

func main() {

	ch := make(chan os.Signal, 1)
	signal.Notify(ch, os.Interrupt, os.Kill)

	go report()

	for {

		c := NewCrawler()
		c.start()

		select {
		case <-time.After(30 * time.Minute):
			// start afresh every 30 Minutes to prefent the address book saturation
			c.close()
			// GC'ing
			c=nil
		case <-ch:
			fmt.Printf("Process interrupted, shutting down...")
			return
		}
	}

}
