package store

import (
	"context"
	"sync"
	"time"

	bbolt "go.etcd.io/bbolt"

	"github.com/shubhamkr585/relay/internal/domain"
)

type opKind int

const (
	kindEnqueue opKind = iota
	kindReserve
	kindAck
	kindNack
	kindExtendLease
	kindRequeueDead
	kindReapAndPromote
)

type writeOp struct {
	kind opKind

	// Parameters
	enqueueParams *EnqueueParams
	queue         string
	workerID      string
	leaseDuration time.Duration
	jobID         string
	leaseToken    string
	errorMsg      string
	retryAfter    time.Duration
	extraDuration time.Duration
	timestampNano int64

	// Result channel
	resCh chan opResult
}

type opResult struct {
	job          *domain.Job
	deduplicated bool
	leaseToken   string
	newExpiresAt int64
	reaped       int
	promoted     int
	err          error
}

// Batcher manages group-commit transactions for concurrent writes.
type Batcher struct {
	db             *bbolt.DB
	cfg            Config
	clock          Clock
	opCh           chan *writeOp
	stopCh         chan struct{}
	wg             sync.WaitGroup
	defaultBackoff func(attempts uint32) time.Duration
}

// newBatcher creates and starts the group-commit micro-batcher.
func newBatcher(db *bbolt.DB, cfg Config, defaultBackoff func(attempts uint32) time.Duration) *Batcher {
	clk := cfg.Clock
	if clk == nil {
		clk = realClock{}
	}

	b := &Batcher{
		db:             db,
		cfg:            cfg,
		clock:          clk,
		opCh:           make(chan *writeOp, 4096),
		stopCh:         make(chan struct{}),
		defaultBackoff: defaultBackoff,
	}

	b.wg.Add(1)
	go b.run()
	return b
}

// submit sends an operation to the batcher and blocks until committed to disk.
func (b *Batcher) submit(ctx context.Context, op *writeOp) (opResult, error) {
	op.resCh = make(chan opResult, 1)

	select {
	case <-b.stopCh:
		return opResult{}, domain.ErrEngineClosed
	case <-ctx.Done():
		return opResult{}, ctx.Err()
	case b.opCh <- op:
	}

	select {
	case <-b.stopCh:
		// Attempt to receive if already processed, otherwise report closed
		select {
		case res := <-op.resCh:
			return res, nil
		default:
			return opResult{}, domain.ErrEngineClosed
		}
	case <-ctx.Done():
		return opResult{}, ctx.Err()
	case res := <-op.resCh:
		return res, nil
	}
}

// run processes batches in a group commit loop.
func (b *Batcher) run() {
	defer b.wg.Done()

	batch := make([]*writeOp, 0, b.cfg.BatchMaxSize)
	timer := time.NewTimer(b.cfg.BatchWindow)
	if !timer.Stop() {
		select {
		case <-timer.C:
		default:
		}
	}
	timerActive := false

	flush := func() {
		if len(batch) == 0 {
			return
		}
		b.commitBatch(batch)
		batch = make([]*writeOp, 0, b.cfg.BatchMaxSize)
		if timerActive {
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}
			timerActive = false
		}
	}

	for {
		select {
		case <-b.stopCh:
			// Drain remaining ops and flush
			for {
				select {
				case op := <-b.opCh:
					batch = append(batch, op)
					if len(batch) >= b.cfg.BatchMaxSize {
						flush()
					}
				default:
					flush()
					return
				}
			}

		case op := <-b.opCh:
			batch = append(batch, op)

			if len(batch) == 1 {
				// First item in batch: start group-commit timer
				timer.Reset(b.cfg.BatchWindow)
				timerActive = true
			}

			if len(batch) >= b.cfg.BatchMaxSize {
				flush()
			}

		case <-timer.C:
			timerActive = false
			flush()
		}
	}
}

// commitBatch executes the collected operations in a single atomic bbolt update transaction.
func (b *Batcher) commitBatch(batch []*writeOp) {
	defaultNowNano := b.clock.Now().UnixNano()
	results := make([]opResult, len(batch))

	dbErr := b.db.Update(func(tx *bbolt.Tx) error {
		for i, op := range batch {
			nowNano := defaultNowNano
			if op.timestampNano > 0 {
				nowNano = op.timestampNano
			}

			var res opResult
			switch op.kind {
			case kindEnqueue:
				job, dedup, err := txEnqueue(tx, op.enqueueParams, nowNano)
				res.job = job
				res.deduplicated = dedup
				res.err = err

			case kindReserve:
				job, token, err := txReserve(tx, op.queue, op.workerID, op.leaseDuration, nowNano, "")
				res.job = job
				res.leaseToken = token
				res.err = err

			case kindAck:
				res.err = txAck(tx, op.jobID, op.leaseToken)

			case kindNack:
				job, err := txNack(tx, op.jobID, op.leaseToken, op.errorMsg, op.retryAfter, nowNano)
				res.job = job
				res.err = err

			case kindExtendLease:
				expiresAt, err := txExtendLease(tx, op.jobID, op.leaseToken, op.extraDuration, nowNano)
				res.newExpiresAt = expiresAt
				res.err = err

			case kindRequeueDead:
				job, err := txRequeueDead(tx, op.jobID, nowNano)
				res.job = job
				res.err = err

			case kindReapAndPromote:
				reaped, err1 := txReapExpiredLeases(tx, nowNano, b.defaultBackoff)
				promoted, err2 := txPromoteScheduled(tx, nowNano)
				res.reaped = reaped
				res.promoted = promoted
				if err1 != nil {
					res.err = err1
				} else {
					res.err = err2
				}
			}

			results[i] = res
		}
		return nil
	})

	for i, op := range batch {
		if dbErr != nil {
			op.resCh <- opResult{err: dbErr}
		} else {
			op.resCh <- results[i]
		}
		close(op.resCh)
	}
}

// stop halts the batcher worker and waits for remaining operations to complete.
func (b *Batcher) stop() {
	close(b.stopCh)
	b.wg.Wait()
}
