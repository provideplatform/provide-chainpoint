package providechainpoint

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/provideservices/provide-go"
)

var (
	daemon *chainptDaemon
)

type chainptDaemon struct {
	q                   chan *[]byte
	bufferSize          int
	flushIntervalMillis uint
	lastFlushTimestamp  time.Time
	mutex               *sync.Mutex
	sleepIntervalMillis uint

	shutdown context.Context
	cancelF  context.CancelFunc
}

// RunChainpointDaemon initializes and starts a new chainpoint daemon and aggregates
// hashes on a buffered channel until the channel is full or a configured timeout
// expires, at which point the entire buffer is flushed by way of provide.SubmitHashes.
//
// Upon successful flushing of the buffer the related in-flight NATS messages are ACK'd. Upon
// timeout of an in-flight NATS message (i.e., a message delivered using the chainpoint.hash
// subject), the message will be attempted for redelivery. There is not currently a
// dead-letter strategy associated with chainpoint daemons (i.e., messages will be retried
// forever unless removed from the NATS streaming broker). A dead-letter queue will be
// considered but needs additional discussion.
//
// This method returns an error if there is already a chainpoint usage daemon running
// in the parent golang process (chainpoint daemon is currently singleton-per-process).
func RunChainpointDaemon(bufferSize int, flushIntervalMillis uint) error {
	if daemon != nil {
		msg := "Attempted to run chainpoint daemon after singleton instance started"
		Log.Warningf(msg)
		return fmt.Errorf(msg)
	}

	daemon = new(chainptDaemon)
	daemon.shutdown, daemon.cancelF = context.WithCancel(context.Background())
	daemon.q = make(chan *[]byte, bufferSize)
	daemon.bufferSize = bufferSize
	daemon.flushIntervalMillis = flushIntervalMillis
	daemon.mutex = &sync.Mutex{}
	daemon.lastFlushTimestamp = time.Now()
	go daemon.run()

	return nil
}

func (d *chainptDaemon) run() error {
	Log.Debugf("Running chainpoint daemon...")
	ticker := time.NewTicker(time.Duration(d.flushIntervalMillis) * time.Millisecond)
	for {
		select {
		case <-ticker.C:
			if len(d.q) > 0 {
				d.flush()
			}
		case <-d.shutdown.Done():
			Log.Debugf("Flushing chainpoint daemon on shutdown")
			ticker.Stop()
			return d.flush()
		}
	}
}

func (d *chainptDaemon) flush() error {
	d.mutex.Lock()
	defer d.mutex.Unlock()

	for {
		select {
		case hashes, ok := <-d.q:
			if ok {
				Log.Debugf("Attempting to flush %d hashes to chainpoint", len(*hashes))
				proofHandles, err := provide.SubmitHashes(*hashes, nil)
				if err != nil {
					Log.Warningf("Failed to receive message from chainpoint daemon; will reattempt submission of %d hashes", len(*hashes))
					d.q <- hashes
					continue
				}
				Log.Debugf("Received %d chainpoint proofs in response to submission of %d hashes", len(proofHandles), len(*hashes))
				// TODO: schedule chainpoint verification verification i.e., NATS "calendar subject" as referenced in architecture.svg
			} else {
				Log.Warningf("Failed to receive message from chainpoint daemon channel")
			}
		default:
			if len(d.q) == 0 {
				Log.Debugf("chainpoint daemon buffered channel flushed")
				return nil
			}
		}
	}
}

// ImmortalizeHashes places an array of hashes (represented as *[]byte at this point)
// on a buffered channel managed by a chainpoint daemon instance. In architecture.svg,
// this method represents the "hash subject" entrypoint and a typical use-case is
// upon receipt of a blockheader.
//
// The provideID is typically-- i.e., when this method is called by the initial Provide
// platform implementation-- the unique identifier of a network on the Provide platform.
// Other implementations can define their own business rules for what provideID actually
// represents at runtime, but it will be used to create a hash with each of the given
// hashes.
func ImmortalizeHashes(provideID string, hashes []*[]byte) error {
	if daemon == nil {
		return fmt.Errorf("Failed to immortalize %d bundles of hashes via chainpoint daemon; singleton daemon instance not yet initialized in this process", len(hashes))
	}

	Log.Debugf("Attempting to immortalize %d hashes via chainpoint daemon", len(hashes))
	for _, hashptr := range hashes {
		hash := provide.Keccak256(fmt.Sprintf("%s.%s", provideID, string(*hashptr)))
		daemon.q <- &hash
	}

	return nil
}
