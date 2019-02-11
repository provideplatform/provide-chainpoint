package providechainpoint

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/provideservices/provide-go"
)

var (
	daemon    *chainptDaemon
	waitGroup sync.WaitGroup
)

type chainptDaemon struct {
	q                   chan []byte        // buffered queue for hashes to immortalize via chainpoint
	pQ                  chan []ProofHandle // buffered queue for proof-of-immortalization (i.e., fetch, verify and store these proofs)
	bufferSize          int
	flushIntervalMillis uint
	lastFlushTimestamp  time.Time
	sleepIntervalMillis uint
	proofIntervalMillis uint

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
func RunChainpointDaemon(bufferSize int, flushIntervalMillis, proofIntervalMillis uint) error {
	if daemon != nil {
		msg := "Attempted to run chainpoint daemon after singleton instance started"
		Log.Warningf(msg)
		return fmt.Errorf(msg)
	}

	go func() {
		waitGroup.Add(1)
		daemon = new(chainptDaemon)
		daemon.shutdown, daemon.cancelF = context.WithCancel(context.Background())
		daemon.bufferSize = bufferSize
		daemon.flushIntervalMillis = flushIntervalMillis
		daemon.proofIntervalMillis = proofIntervalMillis
		daemon.lastFlushTimestamp = time.Now()
		daemon.q = make(chan []byte, bufferSize)
		daemon.pQ = make(chan []ProofHandle, bufferSize)
		go daemon.run()
		waitGroup.Wait()
	}()

	return nil
}

func (d *chainptDaemon) run() error {
	Log.Debugf("Running chainpoint daemon...")
	flushTicker := time.NewTicker(time.Duration(d.flushIntervalMillis) * time.Millisecond)
	proofTicker := time.NewTicker(time.Duration(d.proofIntervalMillis) * time.Millisecond)

	for {
		select {
		case <-flushTicker.C:
			if len(d.q) > 0 {
				d.flushHashes()
			}
		case <-proofTicker.C:
			if len(d.pQ) > 0 {
				d.flushProofs()
			}
		case <-d.shutdown.Done():
			Log.Debugf("Flushing chainpoint daemon on shutdown")
			flushTicker.Stop()
			proofTicker.Stop()
			d.flushProofs()
			if len(d.pQ) > 0 {
				Log.Warningf("Dropping %d immortalized chainpoint proofs which have not been verified", len(d.pQ))
			}
			return d.flushHashes()
		default:
			// no-op
		}
	}
}

func (d *chainptDaemon) flushHashes() error {
	hashes := make([][]byte, 0)

	for {
		select {
		case hash, ok := <-d.q:
			if ok {
				hashes = append(hashes, hash)
			} else {
				Log.Warningf("Failed to receive message from chainpoint daemon channel")
			}
		default:
			// no-op
		}

		if len(hashes) == d.bufferSize {
			break
		}
	}

	Log.Debugf("Hashes... %s", hashes)

	proofHandles, err := SubmitHashes(hashes, nil)
	if err != nil {
		Log.Warningf("Failed to receive message from chainpoint daemon; will reattempt submission of %d hashes; %s", len(hashes), err.Error())
		for _, hash := range hashes {
			d.q <- hash
		}
		return err
	}
	Log.Debugf("Received %d chainpoint proofs in response to submission of %d hashes", len(proofHandles), len(hashes))
	d.pQ <- proofHandles
	// TODO: diff the response against input array; requeue or log failures

	return nil
}

func (d *chainptDaemon) flushProofs() error {
	Log.Debugf("Attempting to flush %d pending chainpoint proofs...", len(d.pQ))
	for {
		select {
		case proofHandles, ok := <-d.pQ:
			if ok {
				Log.Debugf("Attempting to fetch %d proofs from chainpoint", len(proofHandles))
				proofs, err := GetProofs(proofHandles)
				if err != nil {
					Log.Warningf("Failed to receive message from chainpoint daemon; will reattempt fetching %d proofs", len(proofHandles))
					d.pQ <- proofHandles
					continue
				}
				Log.Debugf("Fetched %d chainpoint proofs for verification in response to submission of %d hashes", len(proofs), len(proofHandles))
				// TODO: diff the response against input array; requeue or log failures
				Log.Debugf("Received %d proofs: %s", len(proofs), proofs)
			} else {
				Log.Warningf("Failed to receive message from chainpoint daemon channel")
			}
		default:
			// no-op
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

	Log.Debugf("Attempting to immortalize %d hash(es) via chainpoint daemon", len(hashes))
	for _, hashptr := range hashes {
		hash := provide.Keccak256(fmt.Sprintf("%s.%s", provideID, string(*hashptr)))
		daemon.q <- []byte(common.Bytes2Hex(hash))
		if len(daemon.q) == daemon.bufferSize {
			daemon.flushHashes()
		}
	}

	return nil
}
