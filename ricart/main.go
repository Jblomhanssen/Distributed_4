package main

import (
	"context"
	"fmt"
	"log"
	"net"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"google.golang.org/grpc"
	"ricart/proto"
)

// RicartNode represents a node in the Ricart-Agrawala algorithm
type RicartNode struct {
	proto.UnimplementedRicartServiceServer

	mu            sync.Mutex
	id            int
	timestamp     int64
	replyCount    int
	requesting    bool
	replyDeferred map[int]bool
	peers         map[int]string
	server        *grpc.Server
	protoClients  map[int]proto.RicartServiceClient
	ctx           context.Context
	cancel        context.CancelFunc
}

// --- RPC methods ---

func (n *RicartNode) RequestAccess(ctx context.Context, req *proto.RequestMessage) (*proto.ReplyMessage, error) {
	n.mu.Lock()
	defer n.mu.Unlock()

	fmt.Printf("[Node %d] Received REQUEST from Node %d (ts=%d)\n", n.id, req.NodeId, req.Timestamp)

	if !n.requesting || req.Timestamp < n.timestamp || (req.Timestamp == n.timestamp && req.NodeId < int32(n.id)) {
		// Grant permission immediately
		return &proto.ReplyMessage{NodeId: int32(n.id)}, nil
	}

	// Otherwise defer the reply
	n.replyDeferred[int(req.NodeId)] = true
	fmt.Printf("[Node %d] Deferred reply to Node %d\n", n.id, req.NodeId)
	return &proto.ReplyMessage{NodeId: -1}, nil
}

func (n *RicartNode) ReleaseAccess(ctx context.Context, req *proto.ReleaseMessage) (*proto.ReplyMessage, error) {
	n.mu.Lock()
	defer n.mu.Unlock()

	fmt.Printf("[Node %d] Received RELEASE from Node %d\n", n.id, req.NodeId)
	if n.replyDeferred[int(req.NodeId)] {
		// Send reply now
		go func(peerID int) {
			client := n.protoClients[peerID]
			client.RequestAccess(context.Background(), &proto.RequestMessage{
				NodeId:    int32(n.id),
				Timestamp: time.Now().UnixNano(),
			})
		}(int(req.NodeId))
		delete(n.replyDeferred, int(req.NodeId))
	}
	return &proto.ReplyMessage{NodeId: int32(n.id)}, nil
}

// --- Node logic ---

func (n *RicartNode) startServer(port string) {
	lis, err := net.Listen("tcp", port)
	if err != nil {
		log.Fatalf("Node %d failed to listen: %v", n.id, err)
	}
	n.server = grpc.NewServer()
	proto.RegisterRicartServiceServer(n.server, n)
	fmt.Printf("[Node %d] Listening on %s\n", n.id, port)
	go n.server.Serve(lis)
}

func (n *RicartNode) connectToPeers() {
	for peerID, addr := range n.peers {
		conn, err := grpc.Dial(addr, grpc.WithInsecure())
		if err != nil {
			log.Fatalf("[Node %d] Failed to connect to Node %d: %v", n.id, peerID, err)
		}
		n.protoClients[peerID] = proto.NewRicartServiceClient(conn)
	}
	fmt.Printf("[Node %d] Connected to peers.\n", n.id)
}

func (n *RicartNode) requestCriticalSection() {
	n.mu.Lock()
	n.requesting = true
	n.timestamp = time.Now().UnixNano()
	n.replyCount = 0
	fmt.Printf("[Node %d] Requesting Critical Section (ts=%d)\n", n.id, n.timestamp)
	n.mu.Unlock()

	// Send requests to all peers
	for peerID, client := range n.protoClients {
		go func(peerID int, client proto.RicartServiceClient) {
			_, err := client.RequestAccess(context.Background(), &proto.RequestMessage{
				NodeId:    int32(n.id),
				Timestamp: n.timestamp,
			})
			if err == nil {
				n.mu.Lock()
				n.replyCount++
				n.mu.Unlock()
			}
		}(peerID, client)
	}

	// Wait until all replies received
	for {
		time.Sleep(200 * time.Millisecond)
		n.mu.Lock()
		if n.replyCount >= len(n.protoClients) {
			n.mu.Unlock()
			break
		}
		n.mu.Unlock()
	}

	// Enter critical section
	n.enterCriticalSection()

	// Exit and send release
	n.releaseCriticalSection()
}

func (n *RicartNode) enterCriticalSection() {
	fmt.Printf("🟢 [Node %d] ENTERING Critical Section 🟢\n", n.id)
	time.Sleep(2 * time.Second)
	fmt.Printf("🔴 [Node %d] LEAVING Critical Section 🔴\n", n.id)
}

func (n *RicartNode) releaseCriticalSection() {
	n.mu.Lock()
	n.requesting = false
	n.mu.Unlock()

	for _, client := range n.protoClients {
		client.ReleaseAccess(context.Background(), &proto.ReleaseMessage{
			NodeId: int32(n.id),
		})
	}
}

// --- Main setup ---

func main() {
	if len(os.Args) < 3 {
		fmt.Println("Usage: go run main.go <nodeID> <port> [peerID=addr ...]")
		os.Exit(1)
	}

	nodeID, _ := strconv.Atoi(os.Args[1])
	port := os.Args[2]
	peers := make(map[int]string)

	for _, arg := range os.Args[3:] {
		parts := strings.Split(arg, "=")
		if len(parts) != 2 {
			continue
		}
		id, _ := strconv.Atoi(parts[0])
		peers[id] = parts[1]
	}

	ctx, cancel := context.WithCancel(context.Background())
	node := &RicartNode{
		id:            nodeID,
		replyDeferred: make(map[int]bool),
		peers:         peers,
		protoClients:  make(map[int]proto.RicartServiceClient),
		ctx:           ctx,
		cancel:        cancel,
	}

	node.startServer(port)
	time.Sleep(1 * time.Second)
	node.connectToPeers()

	// Example: each node tries to enter CS after some delay
	time.Sleep(time.Duration(nodeID) * 2 * time.Second)
	node.requestCriticalSection()

	select {}
}
