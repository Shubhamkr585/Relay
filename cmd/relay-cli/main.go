package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	relaypb "github.com/shubhamkr585/relay/api/proto/v1"
)

func main() {
	if len(os.Args) < 2 {
		printUsage()
		os.Exit(1)
	}

	command := os.Args[1]

	serverAddr := os.Getenv("RELAY_SERVER")
	if serverAddr == "" {
		serverAddr = "127.0.0.1:50051"
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	conn, err := grpc.NewClient(serverAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error connecting to server %s: %v\n", serverAddr, err)
		os.Exit(1)
	}
	defer conn.Close()

	client := relaypb.NewRelayServiceClient(conn)

	switch command {
	case "enqueue":
		fs := flag.NewFlagSet("enqueue", flag.ExitOnError)
		serverFlag := fs.String("server", serverAddr, "Relay gRPC server address")
		queue := fs.String("queue", "default", "Queue name")
		payload := fs.String("payload", "", "Job payload")
		priority := fs.Uint("priority", 1, "Job priority (0 = highest)")
		delayMs := fs.Int64("delay", 0, "Schedule delay in milliseconds")
		idempKey := fs.String("idempotency-key", "", "Idempotency deduplication key")
		maxAttempts := fs.Uint("max-attempts", 3, "Maximum delivery attempts")
		_ = fs.Parse(os.Args[2:])

		if *serverFlag != serverAddr {
			conn.Close()
			conn, _ = grpc.NewClient(*serverFlag, grpc.WithTransportCredentials(insecure.NewCredentials()))
			client = relaypb.NewRelayServiceClient(conn)
		}

		if *payload == "" {
			fmt.Fprintln(os.Stderr, "Error: --payload is required")
			os.Exit(1)
		}

		res, err := client.Enqueue(ctx, &relaypb.EnqueueRequest{
			Queue:          *queue,
			Payload:        []byte(*payload),
			Priority:       uint32(*priority),
			DelayMs:        *delayMs,
			IdempotencyKey: *idempKey,
			MaxAttempts:    uint32(*maxAttempts),
		})
		if err != nil {
			fmt.Fprintf(os.Stderr, "Enqueue error: %v\n", err)
			os.Exit(1)
		}

		out, _ := json.MarshalIndent(res, "", "  ")
		fmt.Println(string(out))

	case "reserve":
		fs := flag.NewFlagSet("reserve", flag.ExitOnError)
		serverFlag := fs.String("server", serverAddr, "Relay gRPC server address")
		queue := fs.String("queue", "default", "Queue name")
		workerID := fs.String("worker-id", "cli-worker", "Worker identifier")
		leaseDurationMs := fs.Int64("lease-duration", 30000, "Lease duration in milliseconds")
		_ = fs.Parse(os.Args[2:])

		if *serverFlag != serverAddr {
			conn.Close()
			conn, _ = grpc.NewClient(*serverFlag, grpc.WithTransportCredentials(insecure.NewCredentials()))
			client = relaypb.NewRelayServiceClient(conn)
		}

		res, err := client.Reserve(ctx, &relaypb.ReserveRequest{
			Queue:           *queue,
			WorkerId:        *workerID,
			LeaseDurationMs: *leaseDurationMs,
		})
		if err != nil {
			fmt.Fprintf(os.Stderr, "Reserve error: %v\n", err)
			os.Exit(1)
		}

		out, _ := json.MarshalIndent(res, "", "  ")
		fmt.Println(string(out))

	case "ack":
		fs := flag.NewFlagSet("ack", flag.ExitOnError)
		serverFlag := fs.String("server", serverAddr, "Relay gRPC server address")
		jobID := fs.String("job-id", "", "Job ID to acknowledge")
		leaseToken := fs.String("lease-token", "", "Active lease token")
		_ = fs.Parse(os.Args[2:])

		if *serverFlag != serverAddr {
			conn.Close()
			conn, _ = grpc.NewClient(*serverFlag, grpc.WithTransportCredentials(insecure.NewCredentials()))
			client = relaypb.NewRelayServiceClient(conn)
		}

		if *jobID == "" || *leaseToken == "" {
			fmt.Fprintln(os.Stderr, "Error: --job-id and --lease-token are required")
			os.Exit(1)
		}

		_, err := client.Ack(ctx, &relaypb.AckRequest{
			JobId:      *jobID,
			LeaseToken: *leaseToken,
		})
		if err != nil {
			fmt.Fprintf(os.Stderr, "Ack error: %v\n", err)
			os.Exit(1)
		}
		fmt.Printf("Job %s acknowledged successfully.\n", *jobID)

	case "nack":
		fs := flag.NewFlagSet("nack", flag.ExitOnError)
		serverFlag := fs.String("server", serverAddr, "Relay gRPC server address")
		jobID := fs.String("job-id", "", "Job ID to nack")
		leaseToken := fs.String("lease-token", "", "Active lease token")
		errMsg := fs.String("error", "", "Error message")
		retryAfterMs := fs.Int64("retry-after", 0, "Retry backoff delay in milliseconds")
		_ = fs.Parse(os.Args[2:])

		if *serverFlag != serverAddr {
			conn.Close()
			conn, _ = grpc.NewClient(*serverFlag, grpc.WithTransportCredentials(insecure.NewCredentials()))
			client = relaypb.NewRelayServiceClient(conn)
		}

		if *jobID == "" || *leaseToken == "" {
			fmt.Fprintln(os.Stderr, "Error: --job-id and --lease-token are required")
			os.Exit(1)
		}

		_, err := client.Nack(ctx, &relaypb.NackRequest{
			JobId:        *jobID,
			LeaseToken:   *leaseToken,
			ErrorMsg:     *errMsg,
			RetryAfterMs: *retryAfterMs,
		})
		if err != nil {
			fmt.Fprintf(os.Stderr, "Nack error: %v\n", err)
			os.Exit(1)
		}
		fmt.Printf("Job %s nacked successfully.\n", *jobID)

	case "stats":
		fs := flag.NewFlagSet("stats", flag.ExitOnError)
		serverFlag := fs.String("server", serverAddr, "Relay gRPC server address")
		queue := fs.String("queue", "default", "Queue name")
		_ = fs.Parse(os.Args[2:])

		if *serverFlag != serverAddr {
			conn.Close()
			conn, _ = grpc.NewClient(*serverFlag, grpc.WithTransportCredentials(insecure.NewCredentials()))
			client = relaypb.NewRelayServiceClient(conn)
		}

		res, err := client.Stats(ctx, &relaypb.StatsRequest{Queue: *queue})
		if err != nil {
			fmt.Fprintf(os.Stderr, "Stats error: %v\n", err)
			os.Exit(1)
		}

		out, _ := json.MarshalIndent(res, "", "  ")
		fmt.Println(string(out))

	case "list-dead":
		fs := flag.NewFlagSet("list-dead", flag.ExitOnError)
		serverFlag := fs.String("server", serverAddr, "Relay gRPC server address")
		queue := fs.String("queue", "default", "Queue name")
		limit := fs.Uint("limit", 10, "Maximum number of dead jobs to retrieve")
		_ = fs.Parse(os.Args[2:])

		if *serverFlag != serverAddr {
			conn.Close()
			conn, _ = grpc.NewClient(*serverFlag, grpc.WithTransportCredentials(insecure.NewCredentials()))
			client = relaypb.NewRelayServiceClient(conn)
		}

		res, err := client.ListDead(ctx, &relaypb.ListDeadRequest{
			Queue: *queue,
			Limit: uint32(*limit),
		})
		if err != nil {
			fmt.Fprintf(os.Stderr, "ListDead error: %v\n", err)
			os.Exit(1)
		}

		out, _ := json.MarshalIndent(res, "", "  ")
		fmt.Println(string(out))

	case "requeue-dead":
		fs := flag.NewFlagSet("requeue-dead", flag.ExitOnError)
		serverFlag := fs.String("server", serverAddr, "Relay gRPC server address")
		jobID := fs.String("job-id", "", "Job ID to requeue")
		_ = fs.Parse(os.Args[2:])

		if *serverFlag != serverAddr {
			conn.Close()
			conn, _ = grpc.NewClient(*serverFlag, grpc.WithTransportCredentials(insecure.NewCredentials()))
			client = relaypb.NewRelayServiceClient(conn)
		}

		if *jobID == "" {
			fmt.Fprintln(os.Stderr, "Error: --job-id is required")
			os.Exit(1)
		}

		res, err := client.RequeueDead(ctx, &relaypb.RequeueDeadRequest{JobId: *jobID})
		if err != nil {
			fmt.Fprintf(os.Stderr, "RequeueDead error: %v\n", err)
			os.Exit(1)
		}

		out, _ := json.MarshalIndent(res, "", "  ")
		fmt.Println(string(out))

	default:
		fmt.Fprintf(os.Stderr, "Unknown command: %s\n", command)
		printUsage()
		os.Exit(1)
	}
}

func printUsage() {
	fmt.Println(`Relay CLI - Distributed Durable Job Queue

Usage:
  relay-cli <command> [flags]

Available Commands:
  enqueue       Enqueue a new job
  reserve       Lease the next available job
  ack           Acknowledge job completion
  nack          Negatively acknowledge job (retry or fail)
  stats         Retrieve queue statistics
  list-dead     List dead-lettered jobs
  requeue-dead  Move a dead job back to pending

Global Flags:
  --server      gRPC server address (default: 127.0.0.1:50051 or $RELAY_SERVER)`)
}
