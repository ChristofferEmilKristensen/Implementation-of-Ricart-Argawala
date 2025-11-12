package main

import (
	"context"
	"fmt"
	"log"
	"net"
	"os"
	"slices"
	"strings"
	"sync"
	"time"

	pb "example.com/grpc"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type clientNode struct {
	ID                                  int
	Address                             string
	Peers                               []string // list of peer node addresses
	grpcSrv                             *grpc.Server
	Clock                               int
	IsRequestingAccessToCriticalSection bool
	IsExecutingCriticalSection          bool
	deferredRequests                    []string

	pendingReplies map[string]bool

	mu sync.Mutex

	pb.UnimplementedRicartArgwalaServiceServer
}

func NewNode(id int, address string, peers []string) *clientNode {
	return &clientNode{
		ID:                                  id,
		Address:                             address,
		Peers:                               peers,
		Clock:                               0,
		IsRequestingAccessToCriticalSection: false,
		IsExecutingCriticalSection:          false,
		deferredRequests:                    make([]string, 0),
		pendingReplies:                      initPendingReplies(peers),
	}
}

func initPendingReplies(peers []string) map[string]bool {
	pending := make(map[string]bool)
	for _, addr := range peers {
		pending[addr] = true
	}
	return pending
}

func (n *clientNode) deferRequest(addr string) {
	n.mu.Lock()
	defer n.mu.Unlock()

	// avoid duplicates
	if slices.Contains(n.deferredRequests, addr) {
		return
	}

	n.deferredRequests = append(n.deferredRequests, addr)
	log.Printf("Node %d deferred REPLY to %s", n.ID, addr)
}

// Start launches the gRPC server for this node.
func (n *clientNode) Start() error {
	listener, err := net.Listen("tcp", n.Address)
	if err != nil {
		return fmt.Errorf("failed to listen on %s: %w", n.Address, err)
	}

	n.grpcSrv = grpc.NewServer()
	pb.RegisterRicartArgwalaServiceServer(n.grpcSrv, n)

	log.Printf("clientNode %d listening at %s", n.ID, n.Address)
	return n.grpcSrv.Serve(listener)
}

func (n *clientNode) Stop() {
	if n.grpcSrv != nil {
		n.grpcSrv.GracefulStop()
	}
}

func (n *clientNode) IncrementAndGetIncrementedClock() int {
	n.Clock++
	return n.Clock
}

// RequestAccess handles incoming request messages from peers.
func (n *clientNode) RequestAccess(ctx context.Context, req *pb.RequestMessage) (*pb.Ack, error) {
	log.Printf("clientNode %d received REQUEST from %d (timestamp: %d)", n.ID, req.NodeId, req.LamportTimestamp)

	//update clock
	clock := max(n.Clock, int(req.LamportTimestamp)) + 1
	n.Clock = clock

	//if current node
	if !n.IsExecutingCriticalSection && !n.IsRequestingAccessToCriticalSection {
		//send reply immediata
		n.sendReply(req.FromAddr, req.NodeId)

	} else if n.IsExecutingCriticalSection {
		n.deferRequest(req.FromAddr)

	} else if n.IsRequestingAccessToCriticalSection {
		//add request to queue

		// Case Both nodes want to enter CS — compare timestamps
		if int(req.LamportTimestamp) < n.Clock {
			// The requester has higher priority (smaller timestamp)
			n.sendReply(req.FromAddr, req.NodeId)
		} else {
			// This node has higher priority
			n.deferRequest(req.FromAddr)
		}
	}

	return &pb.Ack{Success: true}, nil
}

func (n *clientNode) sendReply(addr string, nodeID int32) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	conn, err := grpc.Dial(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Printf("Dial %s failed: %v", addr, err)
		return
	}
	defer conn.Close()

	client := pb.NewRicartArgwalaServiceClient(conn)
	reply := &pb.ReplyMessage{
		NodeId:           int32(n.ID),
		LamportTimestamp: int64(n.Clock),
		FromAddr:         n.Address,
	}

	if _, err := client.ReplyAccess(ctx, reply); err != nil {
		log.Printf("ReplyAccess to %s failed: %v", addr, err)
	} else {
		log.Printf("Node %d sent REPLY to %d", n.ID, nodeID)
	}
}

// ReplyAccess handles incoming reply messages from peers.
func (n *clientNode) ReplyAccess(ctx context.Context, reply *pb.ReplyMessage) (*pb.Ack, error) {
	log.Printf("clientNode %d received REPLY from %d (timestamp: %d)", n.ID, reply.NodeId, reply.LamportTimestamp)

	n.mu.Lock()
	defer n.mu.Unlock()

	if int(reply.LamportTimestamp) > n.Clock {
		n.Clock = int(reply.LamportTimestamp)
	}
	n.Clock++

	log.Printf("del pending from %q, left=%d", reply.FromAddr, len(n.pendingReplies)-1)
	delete(n.pendingReplies, reply.FromAddr)

	if len(n.pendingReplies) == 0 && n.IsRequestingAccessToCriticalSection {
		n.IsExecutingCriticalSection = true
		n.IsRequestingAccessToCriticalSection = false
		go n.EnterCriticalSection()
	}

	return &pb.Ack{Success: true}, nil
}

func setupLogger(nodeID int) {
	filename := fmt.Sprintf("node_%d.log", nodeID)
	file, err := os.OpenFile(filename, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		log.Fatalf("Failed to open log file %s: %v", filename, err)
	}
	log.SetOutput(file)
	log.SetFlags(log.LstdFlags | log.Lmicroseconds)
}

// EnterCriticalSection emulates the sensitive operation.
func (n *clientNode) EnterCriticalSection() {

	log.Printf("clientNode %d entering critical section...", n.ID)
	time.Sleep(1 * time.Second)
	log.Println("Critical section: performing sensitive operation")
	time.Sleep(1 * time.Second)
	log.Printf("clientNode %d exiting critical section...", n.ID)

	//cleanup state

	n.mu.Lock()
	n.IsExecutingCriticalSection = false
	n.IsRequestingAccessToCriticalSection = false

	//send replies
	n.releaseDeferredRequests()

	n.pendingReplies = nil

	n.Clock++
	n.mu.Unlock()
}

func (n *clientNode) releaseDeferredRequests() {
	n.mu.Lock()
	deferred := append([]string(nil), n.deferredRequests...) // copy current list
	n.deferredRequests = nil                                 // clear the queue
	n.mu.Unlock()

	for _, addr := range deferred {
		log.Printf("Node %d sending deferred REPLY to %s", n.ID, addr)
		n.sendReply(addr, int32(n.ID))
	}
}

func parseNodeID(id string) int {
	var num int
	_, err := fmt.Sscanf(id, "%d", &num)
	if err != nil {
		log.Fatalf("invalid node ID: %v", err)
	}
	return num
}

func main() {

	requestSent := 0

	time.Sleep(4 * time.Second)
	nodeID := parseNodeID(os.Args[1])
	address := os.Args[2]
	peers := strings.Split(os.Args[3], ",")
	log.Printf("Starting node %s at %s with peers %v", nodeID, address, peers)

	n := NewNode(nodeID, address, peers)
	setupLogger(nodeID)

	// Start the node’s gRPC server in a goroutine
	go func() {
		if err := n.Start(); err != nil {
			log.Fatalf("Node %s failed to start: %v", nodeID, err)
		}
	}()

	// Emulate a node performing actions every few seconds
	for {

		// if requestSent == 3 {
		// 	log.Printf("Does not send request. Got 3 request")
		// 	time.Sleep(10 * time.Second)
		// 	continue
		// }

		time.Sleep(5 * time.Second)
		log.Printf("Node %s requesting access to critical section...", nodeID)

		n.mu.Lock()
		n.pendingReplies = initPendingReplies(n.Peers)
		n.IsRequestingAccessToCriticalSection = true
		n.mu.Unlock()

		timestamp := n.IncrementAndGetIncrementedClock()

		req := &pb.RequestMessage{
			NodeId:           int32(n.ID),
			LamportTimestamp: int64(timestamp),
			FromAddr:         n.Address,
		}

		// Send to all peers concurrently
		var wg sync.WaitGroup

		for _, addr := range n.Peers {
			peer := addr
			wg.Add(1)

			go func() {
				defer wg.Done()

				ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
				defer cancel()

				conn, err := grpc.DialContext(ctx, peer, grpc.WithTransportCredentials(insecure.NewCredentials()))
				if err != nil {
					log.Printf("dial %s: %v", peer, err)
					return
				}
				defer conn.Close()

				client := pb.NewRicartArgwalaServiceClient(conn)

				// send requests
				if _, err := client.RequestAccess(ctx, req); err != nil {
					log.Printf("RequestAccess to %s failed: %v", peer, err)
					return
				}
				log.Printf("Sent REQUEST to %s (ts=%d)", peer, timestamp)

			}()
		}

		requestSent++

		wg.Wait()

		log.Printf("Node %d waiting for REPLYs from peers...", n.ID)

	}

}
