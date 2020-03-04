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

	mu sync.Mutex

	ctx     context.Context
	cancel  context.CancelFunc
	wg      sync.WaitGroup
	limiter <-chan time.Time

	nodes map[peer.ID]*NodeStat

	qlog    *os.File
	connlog *os.File
}

type notifee Crawler

var _ network.Notifiee = (*Crawler)(nil)

func NewCrawler() *Crawler {
	ctx, cancel := context.WithCancel(context.Background())

	c := &Crawler{
		ctx:     ctx,
		cancel:  cancel,
		nodes:   make(map[peer.ID]*NodeStat),
		limiter: time.Tick(2000 * time.Millisecond),
	}

	qlog, err := os.OpenFile("random_queries.log", os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0600)
	if err != nil {
		fmt.Println("Can't open query log file")
		panic(err)
	}
	c.qlog = qlog

	connlog, err := os.OpenFile("conn.log", os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0600)
	if err != nil {
		fmt.Println("Can't open connection log file")
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
	//c.cancel()
	fmt.Println("Shutting down the crawler")
	c.wg.Done()

	if err := c.qlog.Close(); err != nil {
		fmt.Printf("error while closing query log: %v\n", err)
	}
	if err := c.connlog.Close(); err != nil {
		fmt.Printf("error while connection log: %v\n", err)
	}
	if err := c.ds.Close(); err != nil {
		fmt.Printf("error while shutting down: %v\n", err)
	}
	if err := c.host.Close(); err != nil {
		fmt.Printf("error closing connection from host: %v\n", err)
	}
}

func (c *Crawler) start() {
	for i := 0; i < WORKERS; i++ {
		c.wg.Add(1)
		go c.worker()
	}

	go c.reporter()

	c.host.Network().Notify(c)
}

func (c *Crawler) LogNewPeer(id peer.ID) *NodeStat {
	c.mu.Lock()
	defer c.mu.Unlock()

	stats, ok := c.nodes[id]
	if !ok {
		fmt.Printf("Got new peer %v\n", id.Pretty())

		c.nodes[id] = &NodeStat{
			seen: time.Now(),
		}
	} else {
		stats.seen = time.Now()
	}
	return c.nodes[id]
}

func commonPrefix(key string, id peer.ID) int {
	id1 := kbucket.ConvertKey(key)
	id2 := kbucket.ConvertPeerID(id)

	return kbucket.CommonPrefixLen(id1, id2)
}

func (c *Crawler) worker() {
	for {
		<-c.limiter
		randomKey, logKey := getRandomKey()
		fmt.Printf("Querying a random key\n")
		//fmt.Printf("Getting closest peers to: %s\n", randomKey)
		wctx, cancel := context.WithTimeout(c.ctx, 120*time.Second)
		//_, _ = c.dht.FindPeer(ctx, id) // new peers will be saved in the peer store
		peers, err := c.dht.GetClosestPeers(wctx, randomKey)
		if err != nil {
			log.Error(err)
			cancel()
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

			case <-wctx.Done():
				done = true
			}
		}

		// make a random delay between queries
		time.Sleep(time.Duration(1000+mrand.Intn(500)) * time.Millisecond)
		cancel()
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

func (c *Crawler) reporter() {

	for {
		select {
		case <-time.After(10 * time.Second):
			fmt.Printf("--- found %d peers\n", len(c.nodes))
		case <-c.ctx.Done():
			return
		}
	}
}

func (n *Crawler) Connected(net network.Network, conn network.Conn) {
	p := conn.RemotePeer()
	go func() {
		n.id.IdentifyConn(conn)
		<-n.id.IdentifyWait(conn)

		stats := n.LogNewPeer(p)
		stats.connected = time.Now()
		fmt.Fprintf(n.connlog, "%d,%s\n", time.Now().Unix(), p.Pretty())

	}()
}

func (*Crawler) Listen(network.Network, multiaddr.Multiaddr)      {}
func (*Crawler) ListenClose(network.Network, multiaddr.Multiaddr) {}
func (*Crawler) Disconnected(network.Network, network.Conn)       {}
func (*Crawler) OpenedStream(network.Network, network.Stream)     {}
func (*Crawler) ClosedStream(network.Network, network.Stream)     {}

func main() {

	c := NewCrawler()
	defer c.close()
	c.start()

	ch := make(chan os.Signal, 1)
	signal.Notify(ch, os.Interrupt, os.Kill)

	select {
	case <-ch:
		c.close()
		return
	}
}
