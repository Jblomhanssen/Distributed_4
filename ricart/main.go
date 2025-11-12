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

// Shared resource simulation
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
	requesting    bool
	deferredQueue []int
	replyChan     chan int
	peers         map[int]string
	server        *grpc.Server
	clients       map[int]proto.RicartServiceClient
	connections   map[int]*grpc.ClientConn
}

func (n *RicartNode) SendRequest(ctx context.Context, req *proto.RequestMessage) (*proto.Empty, error) {
	n.mu.Lock()
	defer n.mu.Unlock()

	log.Printf("[Node %d] Received request from Node %d (ts=%d)", n.id, req.NodeId, req.Timestamp)

	shouldReply := !n.requesting ||
		req.Timestamp < n.timestamp ||
		(req.Timestamp == n.timestamp && req.NodeId < int32(n.id))

	if shouldReply {
		// Send reply immediately
		go n.sendReplyTo(int(req.NodeId))
	} else {
		// Defer the reply until we exit CS
		log.Printf("[Node %d] Deferring reply to Node %d", n.id, req.NodeId)
		n.deferredQueue = append(n.deferredQueue, int(req.NodeId))
	}

	return &proto.Empty{}, nil
}

func (n *RicartNode) SendReply(ctx context.Context, reply *proto.ReplyMessage) (*proto.Empty, error) {
	log.Printf("[Node %d] Received reply from Node %d", n.id, reply.NodeId)
	n.replyChan <- int(reply.NodeId)
	return &proto.Empty{}, nil
}

func (n *RicartNode) sendReplyTo(nodeID int) {
	client := n.clients[nodeID]
	reply := &proto.ReplyMessage{NodeId: int32(n.id)}
	
	log.Printf("[Node %d] Sending reply to Node %d", n.id, nodeID)
	_, err := client.SendReply(context.Background(), reply)
	if err != nil {
		log.Printf("[Node %d] Error sending reply to %d: %v", n.id, nodeID, err)
	}
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
	n.mu.Lock()
	n.requesting = true
	n.timestamp = time.Now().UnixNano()
	numPeers := len(n.clients)
	log.Printf("[Node %d] Requesting CS with timestamp %d", n.id, n.timestamp)
	n.mu.Unlock()

	// Send request to all peers
	for peerID, client := range n.clients {
		go func(pid int, c proto.RicartServiceClient) {
			req := &proto.RequestMessage{
				NodeId:    int32(n.id),
				Timestamp: n.timestamp,
			}
			_, err := c.SendRequest(context.Background(), req)
			if err != nil {
				log.Printf("[Node %d] Error sending request to %d: %v", n.id, pid, err)
			}
		}(peerID, client)
	}

	// Wait for replies from all peers
	for i := 0; i < numPeers; i++ {
		<-n.replyChan
	}

	log.Printf("[Node %d] Received all replies, entering CS", n.id)

	n.enterCriticalSection()
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

	log.Printf("[Node %d] Read shared counter: %d", n.id, oldValue)
	
	// Simulate some work
	time.Sleep(500 * time.Millisecond)
	
	newValue := oldValue + 1
	
	// Write new value back
	sharedResource.file.Seek(0, 0)
	sharedResource.file.Truncate(0)
	fmt.Fprintf(sharedResource.file, "%d\n", newValue)
	sharedResource.file.Sync()
	
	sharedResource.value = newValue
	sharedResource.mu.Unlock()

	log.Printf("[Node %d] Updated counter: %d -> %d", n.id, oldValue, newValue)
	log.Printf("[Node %d] ===== EXITING CRITICAL SECTION =====", n.id)
}

func (n *RicartNode) exitCriticalSection() {
	n.mu.Lock()
	n.requesting = false
	
	// Get all deferred requests
	deferred := make([]int, len(n.deferredQueue))
	copy(deferred, n.deferredQueue)
	n.deferredQueue = nil
	
	n.mu.Unlock()

	// Now send the deferred replies
	for _, nodeID := range deferred {
		go n.sendReplyTo(nodeID)
	}

	if len(deferred) > 0 {
		log.Printf("[Node %d] Sent %d deferred replies", n.id, len(deferred))
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

	if sharedResource == nil {
		initSharedResource()
	}

	node := &RicartNode{
		id:        nodeID,
		peers:     peers,
		clients:   make(map[int]proto.RicartServiceClient),
		connections: make(map[int]*grpc.ClientConn),
		replyChan: make(chan int, len(peers)),
	}

	node.startServer(port)
	time.Sleep(2 * time.Second)
	node.connectToPeers()
	time.Sleep(1 * time.Second)

	// Stagger requests to create contention
	delay := time.Duration(nodeID) * 500 * time.Millisecond
	log.Printf("[Node %d] Waiting %v before requesting CS", nodeID, delay)
	time.Sleep(delay)

	node.requestCriticalSection()

	time.Sleep(3 * time.Second)
	log.Printf("[Node %d] Requesting CS again", nodeID)
	node.requestCriticalSection()

	select {}
}