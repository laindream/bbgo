package depth

import (
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	log "github.com/sirupsen/logrus"

	"github.com/c9s/bbgo/pkg/types"
	"github.com/c9s/bbgo/pkg/util"
)

type SnapshotFetcher func() (snapshot types.SliceOrderBook, finalUpdateID int64, err error)

type Update struct {
	FirstUpdateID, FinalUpdateID, PreviousFinalUpdateID int64

	// Object is the update object
	Object types.SliceOrderBook
}

//go:generate callbackgen -type Buffer
type Buffer struct {
	buffer []Update

	finalUpdateID int64
	fetcher       SnapshotFetcher
	snapshot      *types.SliceOrderBook

	resetCallbacks []func()
	readyCallbacks []func(snapshot types.SliceOrderBook, updates []Update)
	pushCallbacks  []func(update Update)

	resetC chan struct{}
	mu     sync.Mutex
	once   util.Reonce

	// updateTimeout the timeout duration when not receiving update messages
	updateTimeout time.Duration

	// bufferingPeriod is used to buffer the update message before we get the full depth
	bufferingPeriod atomic.Value

	isFutures bool
}

func (b *Buffer) IsFutures() bool {
	return b.isFutures
}

func (b *Buffer) SetIsFutures(isFutures bool) {
	b.isFutures = isFutures
}

func NewBuffer(fetcher SnapshotFetcher) *Buffer {
	return &Buffer{
		fetcher: fetcher,
		resetC:  make(chan struct{}, 1),
	}
}

func (b *Buffer) SetUpdateTimeout(d time.Duration) {
	b.updateTimeout = d
}

func (b *Buffer) SetBufferingPeriod(d time.Duration) {
	b.bufferingPeriod.Store(d)
}

func (b *Buffer) resetSnapshot() {
	b.snapshot = nil
	b.finalUpdateID = 0
	b.EmitReset()
}

func (b *Buffer) emitReset() {
	select {
	case b.resetC <- struct{}{}:
	default:
	}
}

func (b *Buffer) Reset() {
	b.mu.Lock()
	b.resetSnapshot()
	b.emitReset()
	b.mu.Unlock()
}

// AddUpdate adds the update to the buffer or push the update to the subscriber
func (b *Buffer) AddUpdate(o types.SliceOrderBook, firstUpdateID int64, finalArgs ...int64) error {
	finalUpdateID := firstUpdateID
	if len(finalArgs) > 0 {
		finalUpdateID = finalArgs[0]
	}
	var previousFinalUpdateID int64
	if len(finalArgs) > 1 {
		// previous final update id exists means futures depth
		previousFinalUpdateID = finalArgs[1]
	}
	log.Debugf("get update %d -> %d ,previous final update id %d", firstUpdateID, finalUpdateID, previousFinalUpdateID)

	u := Update{
		FirstUpdateID:         firstUpdateID,
		FinalUpdateID:         finalUpdateID,
		PreviousFinalUpdateID: previousFinalUpdateID,
		Object:                o,
	}

	select {
	case <-b.resetC:
		log.Warnf("received depth reset signal, resetting...")

		// if the once goroutine is still running, overwriting this once might cause "unlock of unlocked mutex" panic.
		b.once.Reset()
	default:
	}

	// if the snapshot is set to nil, we need to buffer the message
	b.mu.Lock()
	if b.snapshot == nil {
		b.buffer = append(b.buffer, u)
		b.once.Do(func() {
			go b.tryFetch()
		})
		b.mu.Unlock()
		return nil
	}

	// if there is a missing update, we should reset the snapshot and re-fetch the snapshot
	if (!b.isFutures && u.FirstUpdateID != b.finalUpdateID+1) ||
		(b.isFutures && previousFinalUpdateID != b.finalUpdateID) {
		// emitReset will reset the once outside the mutex lock section
		b.buffer = []Update{u}
		finalUpdateID = b.finalUpdateID
		if previousFinalUpdateID != 0 {
			finalUpdateID = previousFinalUpdateID
		}
		b.resetSnapshot()
		b.emitReset()
		b.mu.Unlock()
		return fmt.Errorf("found missing update between finalUpdateID %d and firstUpdateID %d, diff: %d",
			finalUpdateID+1,
			u.FirstUpdateID,
			u.FirstUpdateID-finalUpdateID)
	}

	log.Debugf("depth update id %d -> %d", b.finalUpdateID, u.FinalUpdateID)
	b.finalUpdateID = u.FinalUpdateID

	b.mu.Unlock()

	b.EmitPush(u)

	return nil
}

func (b *Buffer) getLastBuffLastUpdateID() int64 {
	var lastUpdateID int64
	b.mu.Lock()
	if len(b.buffer) == 0 {
		b.mu.Unlock()
		return 0
	}
	if b.isFutures {
		lastUpdateID = b.buffer[len(b.buffer)-1].PreviousFinalUpdateID
	} else {
		lastUpdateID = b.buffer[len(b.buffer)-1].FirstUpdateID - 1
	}
	b.mu.Unlock()
	return lastUpdateID
}

func (b *Buffer) fetchAndPush() error {
	book, finalUpdateID, err := b.fetcher()
	if err != nil {
		return err
	}

	for b.getLastBuffLastUpdateID() <= finalUpdateID {
		time.Sleep(50 * time.Millisecond)
	}

	b.mu.Lock()
	log.Debugf("fetched depth snapshot, final update id %d", finalUpdateID)

	if len(b.buffer) > 0 {
		// the snapshot is too early
		if b.isFutures {
			if finalUpdateID < b.buffer[0].PreviousFinalUpdateID {
				b.resetSnapshot()
				b.emitReset()
				b.mu.Unlock()
				return fmt.Errorf("depth snapshot is too early, final update %d is < the first update id %d", finalUpdateID, b.buffer[0].FirstUpdateID)
			}
		} else {
			if finalUpdateID < b.buffer[0].FirstUpdateID-1 {
				b.resetSnapshot()
				b.emitReset()
				b.mu.Unlock()
				return fmt.Errorf("depth snapshot is too early, final update %d is < the first update id %d", finalUpdateID, b.buffer[0].FirstUpdateID)
			}
		}
	}

	var pushUpdates []Update
	if b.isFutures {
		for _, u := range b.buffer {
			// skip old events
			if u.PreviousFinalUpdateID < finalUpdateID {
				continue
			}

			if u.PreviousFinalUpdateID > finalUpdateID {
				b.resetSnapshot()
				b.emitReset()
				b.mu.Unlock()
				return fmt.Errorf("there is a missing depth update, the previous update id %d > final update id %d", u.PreviousFinalUpdateID, finalUpdateID)
			}

			pushUpdates = append(pushUpdates, u)

			// update the final update id to the correct final update id
			finalUpdateID = u.FinalUpdateID
		}
	} else {
		for _, u := range b.buffer {
			// skip old events
			if u.FirstUpdateID < finalUpdateID+1 {
				continue
			}

			if u.FirstUpdateID > finalUpdateID+1 {
				b.resetSnapshot()
				b.emitReset()
				b.mu.Unlock()
				return fmt.Errorf("there is a missing depth update, the update id %d > final update id %d + 1", u.FirstUpdateID, finalUpdateID)
			}

			pushUpdates = append(pushUpdates, u)

			// update the final update id to the correct final update id
			finalUpdateID = u.FinalUpdateID
		}
	}

	// clean the buffer since we have filtered out the buffer we want
	b.buffer = nil

	// set the final update ID so that we will know if there is an update missing
	b.finalUpdateID = finalUpdateID

	// set the snapshot
	b.snapshot = &book

	b.mu.Unlock()

	// should unlock first then call ready
	b.EmitReady(book, pushUpdates)
	return nil
}

func (b *Buffer) tryFetch() {
	for {
		if period := b.bufferingPeriod.Load(); period != nil {
			<-time.After(period.(time.Duration))
		}

		err := b.fetchAndPush()
		if err != nil {
			log.WithError(err).Errorf("snapshot fetch failed")
			continue
		}
		break
	}
}
