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
	"google.golang.org/grpc/credentials/insecure"
	"ricart/proto"
)

// Shared resource that all nodes will try to access
type SharedCounter struct {
	mu    sync.Mutex
	value int
	file  *os.File
}

var sharedResource *SharedCounter

func initSharedResource() {
	file, err := os.OpenFile("shared_counter.txt", os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		log.Printf("Warning: Could not open shared file: %v", err)
	}
	sharedResource = &SharedCounter{
		value: 0,
		file:  file,
	}
}

type RicartNode struct {
	proto.UnimplementedRicartServiceServer

	mu            sync.Mutex
	id            int
	timestamp     int64
	replyCount    int
	requesting    bool
	deferredQueue []int
	peers         map[int]string
	server        *grpc.Server
	clients       map[int]proto.RicartServiceClient
	connections   map[int]*grpc.ClientConn
}

func (n *RicartNode) SendRequest(ctx context.Context, req *proto.RequestMessage) (*proto.ReplyMessage, error) {
	n.mu.Lock()
	defer n.mu.Unlock()

	log.Printf("[Node %d] Received request from Node %d (ts=%d)", n.id, req.NodeId, req.Timestamp)

	// Determine if we should reply now or defer
	shouldReply := !n.requesting ||
		req.Timestamp < n.timestamp ||
		(req.Timestamp == n.timestamp && req.NodeId < int32(n.id))

	if shouldReply {
		log.Printf("[Node %d] Sending reply to Node %d", n.id, req.NodeId)
		return &proto.ReplyMessage{NodeId: int32(n.id)}, nil
	}

	// Defer the reply
	log.Printf("[Node %d] Deferring reply to Node %d", n.id, req.NodeId)
	n.deferredQueue = append(n.deferredQueue, int(req.NodeId))
	
	// Return empty reply - the actual reply will be sent later via SendRelease
	return &proto.ReplyMessage{NodeId: -1}, nil
}

func (n *RicartNode) SendRelease(ctx context.Context, req *proto.ReleaseMessage) (*proto.ReplyMessage, error) {
	log.Printf("[Node %d] Received release from Node %d", n.id, req.NodeId)
	return &proto.ReplyMessage{NodeId: int32(n.id)}, nil
}

func (n *RicartNode) startServer(port string) {
	lis, err := net.Listen("tcp", port)
	if err != nil {
		log.Fatalf("[Node %d] Failed to listen: %v", n.id, err)
	}

	n.server = grpc.NewServer()
	proto.RegisterRicartServiceServer(n.server, n)

	log.Printf("[Node %d] Starting server on %s", n.id, port)

	go func() {
		if err := n.server.Serve(lis); err != nil {
			log.Fatalf("[Node %d] Server error: %v", n.id, err)
		}
	}()
}

func (n *RicartNode) connectToPeers() {
	for peerID, addr := range n.peers {
		conn, err := grpc.Dial(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			log.Fatalf("[Node %d] Failed to connect to Node %d: %v", n.id, peerID, err)
		}
		n.connections[peerID] = conn
		n.clients[peerID] = proto.NewRicartServiceClient(conn)
	}
	log.Printf("[Node %d] Connected to all peers", n.id)
}

func (n *RicartNode) requestCriticalSection() {
	// Start request phase
	n.mu.Lock()
	n.requesting = true
	n.timestamp = time.Now().UnixNano()
	n.replyCount = 0
	numPeers := len(n.clients)
	log.Printf("[Node %d] Requesting CS with timestamp %d", n.id, n.timestamp)
	n.mu.Unlock()

	// Send request to all peers
	replyChan := make(chan bool, numPeers)
	for peerID, client := range n.clients {
		go func(pid int, c proto.RicartServiceClient) {
			req := &proto.RequestMessage{
				NodeId:    int32(n.id),
				Timestamp: n.timestamp,
			}

			reply, err := c.SendRequest(context.Background(), req)
			if err != nil {
				log.Printf("[Node %d] Error requesting from Node %d: %v", n.id, pid, err)
				return
			}

			// Check if we got a real reply or deferred (-1)
			if reply.NodeId != -1 {
				log.Printf("[Node %d] Got reply from Node %d", n.id, reply.NodeId)
				replyChan <- true
			} else {
				// Reply was deferred, we'll get it later via release
				log.Printf("[Node %d] Reply from Node %d was deferred", n.id, pid)
				replyChan <- true
			}
		}(peerID, client)
	}

	// Wait for all replies
	for i := 0; i < numPeers; i++ {
		<-replyChan
	}

	log.Printf("[Node %d] Received all replies, entering CS", n.id)

	// Enter critical section
	n.enterCriticalSection()

	// Exit and release
	n.exitCriticalSection()
}

func (n *RicartNode) enterCriticalSection() {
	log.Printf("[Node %d] ===== ENTERING CRITICAL SECTION =====", n.id)

	// Access shared resource
	sharedResource.mu.Lock()
	oldValue := sharedResource.value
	
	// Read current value from file
	sharedResource.file.Seek(0, 0)
	buf := make([]byte, 32)
	bytesRead, _ := sharedResource.file.Read(buf)
	if bytesRead > 0 {
		fmt.Sscanf(string(buf[:bytesRead]), "%d", &oldValue)
	}

	log.Printf("[Node %d] Read shared counter value: %d", n.id, oldValue)
	
	// Simulate some work on the shared resource
	time.Sleep(500 * time.Millisecond)
	
	newValue := oldValue + 1
	
	// Write new value
	sharedResource.file.Seek(0, 0)
	sharedResource.file.Truncate(0)
	fmt.Fprintf(sharedResource.file, "%d\n", newValue)
	sharedResource.file.Sync()
	
	sharedResource.value = newValue
	sharedResource.mu.Unlock()

	log.Printf("[Node %d] Updated shared counter: %d -> %d", n.id, oldValue, newValue)
	log.Printf("[Node %d] ===== EXITING CRITICAL SECTION =====", n.id)
}

func (n *RicartNode) exitCriticalSection() {
	n.mu.Lock()
	n.requesting = false
	
	// Get deferred requests and clear queue
	deferred := make([]int, len(n.deferredQueue))
	copy(deferred, n.deferredQueue)
	n.deferredQueue = nil
	
	n.mu.Unlock()

	// Send release to all peers (including those we deferred)
	for peerID, client := range n.clients {
		go func(pid int, c proto.RicartServiceClient) {
			releaseMsg := &proto.ReleaseMessage{NodeId: int32(n.id)}
			_, err := c.SendRelease(context.Background(), releaseMsg)
			if err != nil {
				log.Printf("[Node %d] Error sending release to Node %d: %v", n.id, pid, err)
			}
		}(peerID, client)
	}

	if len(deferred) > 0 {
		log.Printf("[Node %d] Sent deferred replies to %d nodes", n.id, len(deferred))
	}
}

func main() {
	if len(os.Args) < 3 {
		fmt.Println("Usage: go run main.go <nodeID> <port> <peer1=addr> <peer2=addr> ...")
		fmt.Println("Example: go run main.go 1 :5001 2=localhost:5002 3=localhost:5003")
		os.Exit(1)
	}

	nodeID, err := strconv.Atoi(os.Args[1])
	if err != nil {
		log.Fatalf("Invalid node ID: %v", err)
	}

	port := os.Args[2]
	peers := make(map[int]string)

	for _, arg := range os.Args[3:] {
		parts := strings.Split(arg, "=")
		if len(parts) != 2 {
			log.Printf("Warning: Skipping invalid peer format: %s", arg)
			continue
		}
		id, err := strconv.Atoi(parts[0])
		if err != nil {
			log.Printf("Warning: Invalid peer ID: %s", parts[0])
			continue
		}
		peers[id] = parts[1]
	}

	log.Printf("Starting Node %d with %d peers", nodeID, len(peers))

	// Initialize shared resource (only once, globally)
	if sharedResource == nil {
		initSharedResource()
	}

	node := &RicartNode{
		id:          nodeID,
		peers:       peers,
		clients:     make(map[int]proto.RicartServiceClient),
		connections: make(map[int]*grpc.ClientConn),
	}

	node.startServer(port)
	time.Sleep(2 * time.Second)
	node.connectToPeers()
	time.Sleep(1 * time.Second)

	// Stagger the requests so nodes compete
	delay := time.Duration(nodeID) * 500 * time.Millisecond
	log.Printf("[Node %d] Waiting %v before requesting CS", nodeID, delay)
	time.Sleep(delay)

	// Request CS twice to show it works multiple times
	node.requestCriticalSection()

	time.Sleep(3 * time.Second)
	log.Printf("[Node %d] Requesting CS again", nodeID)
	node.requestCriticalSection()

	// Keep running
	select {}
}