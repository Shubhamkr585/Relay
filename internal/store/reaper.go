package store

import (
	"context"
	"sync"
	"time"
)

// Reaper is the background process responsible for sweeping expired leases
// and promoting matured scheduled jobs into the ready index.
type Reaper struct {
	batcher  *Batcher
	interval time.Duration
	stopCh   chan struct{}
	wg       sync.WaitGroup
}

// newReaper creates and starts the background lease reaper.
func newReaper(batcher *Batcher, interval time.Duration) *Reaper {
	if interval <= 0 {
		interval = 50 * time.Millisecond
	}

	r := &Reaper{
		batcher:  batcher,
		interval: interval,
		stopCh:   make(chan struct{}),
	}

	r.wg.Add(1)
	go r.run()
	return r
}

func (r *Reaper) run() {
	defer r.wg.Done()
	ticker := time.NewTicker(r.interval)
	defer ticker.Stop()

	for {
		select {
		case <-r.stopCh:
			return
		case <-ticker.C:
			ctx, cancel := context.WithTimeout(context.Background(), r.interval)
			_, _ = r.batcher.submit(ctx, &writeOp{kind: kindReapAndPromote})
			cancel()
		}
	}
}

// ReapAndPromoteOnce manually triggers a single sweep, useful for tests.
func (r *Reaper) ReapAndPromoteOnce(ctx context.Context) (int, int, error) {
	res, err := r.batcher.submit(ctx, &writeOp{kind: kindReapAndPromote})
	if err != nil {
		return 0, 0, err
	}
	return res.reaped, res.promoted, res.err
}

// ReapAndPromoteAt manually triggers a sweep evaluated at timestamp now.
func (r *Reaper) ReapAndPromoteAt(ctx context.Context, now time.Time) (int, int, error) {
	res, err := r.batcher.submit(ctx, &writeOp{
		kind:          kindReapAndPromote,
		timestampNano: now.UnixNano(),
	})
	if err != nil {
		return 0, 0, err
	}
	return res.reaped, res.promoted, res.err
}

// stop halts the background reaper.
func (r *Reaper) stop() {
	close(r.stopCh)
	r.wg.Wait()
}
