package server

import (
	"encoding/json"
	"net/http"

	"github.com/hashicorp/raft"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	relaypb "github.com/shubhamkr585/relay/api/proto/v1"
)

// SetupHTTPHandler creates an http.Handler with /metrics, /healthz, /readyz, and /join routes.
func (s *Server) SetupHTTPHandler() http.Handler {
	mux := http.NewServeMux()

	// 1. Prometheus /metrics
	if s.metrics != nil && s.metrics.Registry != nil {
		mux.Handle("/metrics", promhttp.HandlerFor(s.metrics.Registry, promhttp.HandlerOpts{}))
	}

	// 2. Liveness /healthz
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"status":"ok"}`))
	})

	// 3. Readiness /readyz
	mux.HandleFunc("/readyz", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")

		if s.raft == nil {
			w.WriteHeader(http.StatusServiceUnavailable)
			_, _ = w.Write([]byte(`{"ready":false,"reason":"raft not initialized"}`))
			return
		}

		leader := s.raft.Leader()
		if leader == "" {
			w.WriteHeader(http.StatusServiceUnavailable)
			_, _ = w.Write([]byte(`{"ready":false,"reason":"no leader elected"}`))
			return
		}

		cfgFuture := s.raft.GetConfiguration()
		if err := cfgFuture.Error(); err != nil {
			w.WriteHeader(http.StatusServiceUnavailable)
			_, _ = w.Write([]byte(`{"ready":false,"reason":"failed to get raft configuration"}`))
			return
		}

		inCluster := false
		for _, srv := range cfgFuture.Configuration().Servers {
			if srv.ID == raft.ServerID(s.nodeID) {
				inCluster = true
				break
			}
		}

		if !inCluster {
			w.WriteHeader(http.StatusServiceUnavailable)
			_, _ = w.Write([]byte(`{"ready":false,"reason":"node not yet part of raft cluster"}`))
			return
		}

		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"ready":true}`))
	})

	// 4. HTTP /join
	mux.HandleFunc("/join", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}

		var req relaypb.JoinRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, "invalid json payload: "+err.Error(), http.StatusBadRequest)
			return
		}

		resp, err := s.Join(r.Context(), &req)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_ = json.NewEncoder(w).Encode(resp)
	})

	return mux
}
