package server

import (
	"context"
	"log/slog"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/status"
)

// UnaryLoggingAndMetricsInterceptor returns a gRPC unary interceptor that logs requests
// via log/slog and records RPC durations in Prometheus.
func UnaryLoggingAndMetricsInterceptor(logger *slog.Logger, metrics *Metrics) grpc.UnaryServerInterceptor {
	return func(
		ctx context.Context,
		req interface{},
		info *grpc.UnaryServerInfo,
		handler grpc.UnaryHandler,
	) (interface{}, error) {
		start := time.Now()

		resp, err := handler(ctx, req)

		duration := time.Since(start)
		st, _ := status.FromError(err)
		code := st.Code()

		if metrics != nil {
			metrics.RPCDuration.WithLabelValues(info.FullMethod).Observe(duration.Seconds())
		}

		if logger != nil {
			attrs := []any{
				slog.String("method", info.FullMethod),
				slog.String("code", code.String()),
				slog.Float64("duration_ms", float64(duration.Microseconds())/1000.0),
			}
			if err != nil {
				attrs = append(attrs, slog.String("error", err.Error()))
				logger.Warn("gRPC request handled with error", attrs...)
			} else {
				logger.Info("gRPC request handled", attrs...)
			}
		}

		return resp, err
	}
}
