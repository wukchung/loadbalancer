package balancer

import (
	"context"
	"fmt"
	"sort"
	"sync"

	"github.com/google/uuid"
)

type Client interface {
	// Weight is unit-less number that determines how much processing capacity can a client be allocated
	// when running in parallel with other clients. The higher the weight, the more capacity the client receives.
	Weight() int
	// Workload returns a channel of work chunks that are ment to be processed through the Server.
	// Client's channel is always filled with work chunks.
	Workload(ctx context.Context) chan int
}

// Server defines methods required to process client's work chunks (requests).
type Server interface {
	// Process takes one work chunk (request) and does something with it. The error can be ignored.
	Process(ctx context.Context, workChunk int) error
}

// Balancer makes sure the Server is not smashed with incoming requests (work chunks) by only enabling certain number
// of parallel requests processed by the Server. Imagine there's a SLO defined, and we don't want to make the expensive
// service people angry.
//
// If implementing more advanced balancer, ake sure to correctly assign processing capacity to a client based on other
// clients currently in process.
// To give an example of this, imagine there's a maximum number of work chunks set to 100 and there are two clients
// registered, both with the same priority. When they are both served in parallel, each of them gets to send
// 50 chunks at the same time.
// In the same scenario, if there were two clients with priority 1 and one client with priority 2, the first
// two would be allowed to send 25 requests and the other one would send 50. It's likely that the one sending 50 would
// be served faster, finishing the work early, meaning that it would no longer be necessary that those first two
// clients only send 25 each but can and should use the remaining capacity and send 50 again.
type Balancer struct {
	// limiter is a channel that is used to limit the number of parallel requests processed by the Server.
	limiter chan struct{}

	// service is the Server that the Balancer is balancing for.
	service Server

	// totalWeight is the sum of all clients' weights. It's used to calculate the share of the processing capacity
	totalWeight int

	// maxLoad is the maximum number of work chunks that can be processed by the Server in parallel.
	maxLoad int32

	// lock is used to protect the totalWeight field.
	lock *sync.Mutex

	// capacityCond is used to signal that there is a potentional for free capacity
	capacityCond *sync.Cond

	// clientsStatus is a slice of ClientStatus that is used to keep track of the clientsStatus' status
	// added only for the print out purposes
	clientsStatus map[string]*ClientStatus

	// print lock - added only for the print out purposes
	printLock *sync.Mutex
}

// ClientStatus is used to keep track of the client's status
type ClientStatus struct {
	// clientLock is used to protect the clientLoad field
	clientLock *sync.RWMutex

	// clientLoad is used to keep track of how many tasks are currently being processed by the client
	clientLoad int32

	// capacityWaitLock is used to protect the capacityWait field
	capacityWaitLock *sync.Mutex

	// client weight - added only for the print out purposes
	weight int

	// queued is used to keep track of how many tasks are currently queued - added only for the print out purposes
	queued int

	// processed is used to keep track of how many tasks are already processed - added only for the print out purposes
	processed int
}

// UpdateProcessed updates the number of processed tasks
func (c *ClientStatus) UpdateProcessed(i int) {
	c.clientLock.Lock()
	defer c.clientLock.Unlock()
	c.processed += i
}

// UpdateQueued updates the number of queued tasks
func (c *ClientStatus) UpdateQueued(i int) {
	c.clientLock.Lock()
	defer c.clientLock.Unlock()
	c.queued += i
}

// UpdateLoad updates the number of tasks currently being processed
func (c *ClientStatus) UpdateLoad(i int32) {
	c.clientLock.Lock()
	defer c.clientLock.Unlock()
	c.clientLoad += i
}

// GetLoad returns the number of tasks currently being processed
func (c *ClientStatus) GetLoad() int32 {
	c.clientLock.RLock()
	defer c.clientLock.RUnlock()
	return c.clientLoad
}

// New creates a new Balancer instance. It needs the server that it's going to balance for and a maximum number of work
// chunks that can the processor process at a time. THIS IS A HARD REQUIREMENT - THE SERVICE CANNOT PROCESS MORE THAN
// <PROVIDED NUMBER> OF WORK CHUNKS IN PARALLEL.
func New(service Server, maxLoad int32) *Balancer {
	return &Balancer{
		service:       service,
		limiter:       make(chan struct{}, maxLoad),
		maxLoad:       maxLoad,
		lock:          &sync.Mutex{},
		capacityCond:  sync.NewCond(&sync.Mutex{}),
		clientsStatus: make(map[string]*ClientStatus),
		printLock:     &sync.Mutex{},
	}
}

// Register a client to the balancer and start processing its work chunks through provided processor (server).
// For the sake of simplicity, assume that the client has no identifier, meaning the same client can register themselves
// multiple times.
func (b *Balancer) Register(ctx context.Context, client Client) {
	b.lock.Lock()
	b.totalWeight += client.Weight()
	b.lock.Unlock()

	go b.consumeWorkload(ctx, client.Workload(ctx), client.Weight())
}

// addClient adds a new client to the balancer
// generate a random client ID
// added only for the print out purposes
func (b *Balancer) addClientStatus(clientStatus *ClientStatus) string {
	cUUID, _ := uuid.NewUUID()
	clientID := cUUID.String()[0:8]
	b.lock.Lock()
	defer b.lock.Unlock()
	b.clientsStatus[clientID] = clientStatus
	return clientID
}

// consumeWorkload consumes the workload from the client's channel
func (b *Balancer) consumeWorkload(ctx context.Context, workload chan int, weight int) {
	// wg is used to wait for all goroutines to finish when channel is closed
	// this is important to avoid OOM
	wg := sync.WaitGroup{}

	clientStatus := &ClientStatus{
		clientLock:       &sync.RWMutex{},
		clientLoad:       0,
		capacityWaitLock: &sync.Mutex{},
		weight:           weight,
	}

	clientID := b.addClientStatus(clientStatus)

	for {
		select {
		case <-ctx.Done():
			// this is questionable
			// the main is going to exit anyway but we should probably wait for all goroutines to finish
			// I'll not chase this rabbit in a scope of this task
			//b.gracefullShutdown(&wg, weight, clientID)
			return
		case chunk, ok := <-workload:
			if !ok {
				// we want to finish all the work after the channel is closed
				b.gracefullShutdown(&wg, weight, clientID)
				return
			}
			wg.Add(1)
			clientStatus.UpdateQueued(1)
			go func() {
				// decrease the wait group counter when the goroutine finishes
				defer wg.Done()

				// block until there is enough capacity to process the task
				b.getCap(clientStatus, weight)

				// process the task
				err := b.service.Process(ctx, chunk)
				if err != nil {
					// a bit dump but we are not going to handle the error any further
					fmt.Println("Error processing the task:", err)
				}

				// release the capacity
				clientStatus.UpdateLoad(-1)
				<-b.limiter

				// signal that the task has been released
				b.capacityCond.Broadcast()

				// update the client status
				clientStatus.UpdateQueued(-1)
				clientStatus.UpdateProcessed(1)
				b.PrintClientStatus()
			}()
		}
	}
}

// gracefullShutdown waits for all goroutines for a specific client to finish and then "unregisters" the client
func (b *Balancer) gracefullShutdown(wg *sync.WaitGroup, weight int, clientID string) {
	// wait for all goroutines to finish
	wg.Wait()

	b.lock.Lock()
	defer b.lock.Unlock()

	// unregister the client, since we are not using any identifiers, we are going to assume that the client is unregistered by
	// reducing the totalWeight
	b.totalWeight -= weight

	// remove the client from the clients map
	delete(b.clientsStatus, clientID)

	// signal client has been unregistered and capacity could be free
	b.capacityCond.Broadcast()
}

// clientCap calculates the client's share of the processing capacity
// I work with integers and just let the division to round down.
// It can lead to some inaccuracy and leaving some capacity unused, but I just want to keep it simple
func (b *Balancer) clientCap(weight int) int32 {
	b.lock.Lock()
	defer b.lock.Unlock()

	// this should not happen, but just in case
	if b.totalWeight == 0 {
		return 0
	}

	return int32(weight) * b.maxLoad / int32(b.totalWeight)
}

// getCap waits until there is enough capacity to process the task
// There are two different conditions that need to be met:
// 1. The client's share of the processing capacity is higher than the number of tasks currently being processed
// 2. Maximum number of tasks being processed is not exceeded
func (b *Balancer) getCap(clientStatus *ClientStatus, weight int) {
	// we need to make sure only one goroutine is getting capacity at a time for the same client
	clientStatus.capacityWaitLock.Lock()
	defer clientStatus.capacityWaitLock.Unlock()

	for {
		// calculate the client's share of the processing capacity
		clientShare := b.clientCap(weight)

		// check if the client's share of the processing capacity is higher than the number of tasks currently being processed
		load := clientStatus.GetLoad()
		if load < clientShare {
			b.limiter <- struct{}{}
			clientStatus.UpdateLoad(1)
			return
		}

		// we have two different conditions
		// 1. Specific client finished processing a task and released the capacity
		// 2. Any client got unregistered and there could be a free capacity
		b.capacityCond.L.Lock()
		b.capacityCond.Wait()
		b.capacityCond.L.Unlock()
	}
}

// PrintClientStatus prints the status of all clients in the balancer
const (
	Reset      = "\033[0m"
	Red        = "\033[31m"
	Green      = "\033[32m"
	Yellow     = "\033[33m"
	Blue       = "\033[34m"
	Magenta    = "\033[35m"
	Cyan       = "\033[36m"
	Bold       = "\033[1m"
	ClearLine  = "\033[2K"
	MoveUpLine = "\033[F"
)

// We should not used global variables, but I'm going to use it for the sake of simplicity
var previousLines int = 0

// PrintClientStatus prints the status of all clients in the balancer
// COPY PASTED, NOT PART OF THE TASK
// I didn't spend time on this, I just chatGPTed it and little modifed to my needs
// THIS FUNCTION IS NOT CONCURRENT SAFE
func (b *Balancer) PrintClientStatus() {
	b.printLock.Lock()
	defer b.printLock.Unlock()

	for i := 0; i < previousLines; i++ {
		fmt.Print(MoveUpLine + ClearLine)
	}

	// get the keys
	keys := make([]string, 0, len(b.clientsStatus))
	for k := range b.clientsStatus {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	for _, key := range keys {
		clientStatus := b.clientsStatus[key]

		clientWeight := clientStatus.weight
		clientShare := b.clientCap(clientWeight)

		load := clientStatus.GetLoad()
		dots := ""
		for j := int32(0); j < load; j++ {
			dots += "."
		}

		color := Blue
		if load == clientShare {
			color = Yellow
		} else if load > clientShare {
			color = Red
		} else {
			color = Green
		}

		// Print client status with colors
		fmt.Printf(ClearLine+"Client %s (weight: %s%d%s, queued: %d, processed: %d) [%s%s%s] %s%d%s/%s%d%s\n",
			key,
			Bold, clientWeight, Reset,
			clientStatus.queued,
			clientStatus.processed,
			color, dots, Reset,
			Magenta, load, Reset,
			Cyan, clientShare, Reset,
		)
	}

	previousLines = len(b.clientsStatus)
}
