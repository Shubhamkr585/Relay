package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"math/rand"
	"os"
	"os/signal"
	"syscall"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"

	relaypb "github.com/shubhamkr585/relay/api/proto/v1"
)

// ExampleWorker demonstrates robust consumer-side best practices for Relay:
// 1. Monotonic Fencing Tokens: passing lease_token with Ack/Nack to guard against split-brain execution.
// 2. Client-Side Exponential Backoff: eliminating empty-polling busy loops on empty queues.
// 3. Consumer Idempotency: using Job ID / Idempotency Key for downstream atomic deduplication.
// 4. Retry & Dead-Letter Handling: explicit Nack with backoff, graduating to DLQ upon max attempts.
func main() {
	serverAddr := "127.0.0.1:50051"
	if envAddr := os.Getenv("RELAY_SERVER"); envAddr != "" {
		serverAddr = envAddr
	}

	queueName := "default"
	if envQueue := os.Getenv("RELAY_QUEUE"); envQueue != "" {
		queueName = envQueue
	}

	workerID := fmt.Sprintf("worker-%d-%d", os.Getpid(), rand.Intn(10000))
	log.Printf("[Worker %s] Connecting to Relay at %s (queue: %s)...", workerID, serverAddr, queueName)

	conn, err := grpc.NewClient(serverAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("Failed to connect to Relay server: %v", err)
	}
	defer conn.Close()

	client := relaypb.NewRelayServiceClient(conn)

	// Graceful shutdown handling
	ctx, cancel := context.WithCancel(context.Background())
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-sigCh
		log.Println("Shutdown signal received, draining active job...")
		cancel()
	}()

	// Polling backoff configuration
	baseDelay := 50 * time.Millisecond
	maxDelay := 1000 * time.Millisecond
	currentDelay := baseDelay

	log.Printf("[Worker %s] Started polling loop. Press Ctrl+C to stop.", workerID)

	for {
		select {
		case <-ctx.Done():
			log.Println("Worker stopped gracefully.")
			return
		default:
		}

		// 1. Reserve job with visibility lease timeout (e.g. 10 seconds)
		reqCtx, reqCancel := context.WithTimeout(ctx, 3*time.Second)
		res, err := client.Reserve(reqCtx, &relaypb.ReserveRequest{
			Queue:           queueName,
			WorkerId:        workerID,
			LeaseDurationMs: 10000, // 10s lease
		})
		reqCancel()

		if err != nil {
			st, ok := status.FromError(err)
			if ok && st.Code() == codes.NotFound {
				// Queue is currently empty: apply jittered exponential backoff
				jitter := time.Duration(rand.Int63n(int64(currentDelay / 4)))
				time.Sleep(currentDelay + jitter)
				currentDelay *= 2
				if currentDelay > maxDelay {
					currentDelay = maxDelay
				}
				continue
			}

			// Transient cluster error or failover in progress: brief sleep and reset backoff
			log.Printf("[Worker %s] Reserve error (%v), retrying in 500ms...", workerID, err)
			time.Sleep(500 * time.Millisecond)
			continue
		}

		// Reset backoff upon successful reservation
		currentDelay = baseDelay
		job := res.Job
		leaseToken := res.LeaseToken

		log.Printf("[Worker %s] Reserved job %s (key: %s, attempt: %d/%d, token: %s)",
			workerID, job.Id, job.IdempotencyKey, job.Attempts, job.MaxAttempts, leaseToken)

		// 2. Execute the task with idempotent side effects
		err = processTask(ctx, job)
		if err != nil {
			log.Printf("[Worker %s] Task processing failed for job %s: %v", workerID, job.Id, err)

			// 3. On failure: Nack with monotonic fencing token and retry backoff
			nackCtx, nackCancel := context.WithTimeout(context.Background(), 3*time.Second)
			_, nackErr := client.Nack(nackCtx, &relaypb.NackRequest{
				JobId:        job.Id,
				LeaseToken:   leaseToken,
				ErrorMsg:     err.Error(),
				RetryAfterMs: 2000, // retry after 2 seconds
			})
			nackCancel()

			if nackErr != nil {
				st, _ := status.FromError(nackErr)
				if st.Code() == codes.FailedPrecondition {
					log.Printf("[Worker %s] Nack rejected: lease expired while processing job %s", workerID, job.Id)
				} else {
					log.Printf("[Worker %s] Nack failed: %v", workerID, nackErr)
				}
			}
			continue
		}

		// 4. On success: Ack with monotonic fencing token
		ackCtx, ackCancel := context.WithTimeout(context.Background(), 3*time.Second)
		_, ackErr := client.Ack(ackCtx, &relaypb.AckRequest{
			JobId:      job.Id,
			LeaseToken: leaseToken,
		})
		ackCancel()

		if ackErr != nil {
			st, _ := status.FromError(ackErr)
			if st.Code() == codes.FailedPrecondition {
				// Fencing token protection active: our lease expired before we called Ack!
				log.Printf("[Worker %s] STALE LEASE REJECTED: lease expired before Ack for job %s. Discarding side effect.", workerID, job.Id)
			} else {
				log.Printf("[Worker %s] Ack failed: %v", workerID, ackErr)
			}
		} else {
			log.Printf("[Worker %s] Successfully processed and acknowledged job %s", workerID, job.Id)
		}
	}
}

// processTask performs simulated work with consumer-side idempotency.
func processTask(ctx context.Context, job *relaypb.Job) error {
	// Applications should execute downstream side-effects inside an atomic transaction
	// keyed on job.Id or job.IdempotencyKey (e.g. INSERT ... ON CONFLICT DO NOTHING).
	select {
	case <-time.After(50 * time.Millisecond):
		// Simulated fast processing
		return nil
	case <-ctx.Done():
		return errors.New("processing aborted: context cancelled")
	}
}
