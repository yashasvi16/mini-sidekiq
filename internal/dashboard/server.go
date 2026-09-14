// Package dashboard implements the HTTP API and embedded web UI for
// inspecting queues and jobs. It deliberately uses only net/http's
// method+pattern routing (added in Go 1.22 - "GET /jobs/{id}") instead of
// the plan's suggested chi router: since the standard library now does
// exactly what's needed, adding a routing dependency wouldn't buy anything.
package dashboard

import (
	"embed"
	"net/http"

	"github.com/prometheus/client_golang/prometheus/promhttp"

	"sidekiq/internal/broker"
)

//go:embed ui/index.html
var uiFS embed.FS

// Server serves the dashboard's REST API, Prometheus metrics endpoint, and
// the static UI, all against a single Broker.
type Server struct {
	broker broker.Broker
	mux    *http.ServeMux
}

// NewServer builds a dashboard Server backed by b.
func NewServer(b broker.Broker) *Server {
	s := &Server{broker: b, mux: http.NewServeMux()}
	s.routes()
	return s
}

func (s *Server) routes() {
	s.mux.HandleFunc("GET /api/v1/queues", s.handleGetQueues)
	s.mux.HandleFunc("GET /api/v1/jobs", s.handleListJobs)
	s.mux.HandleFunc("GET /api/v1/jobs/{id}", s.handleGetJob)
	s.mux.HandleFunc("POST /api/v1/jobs/{id}/retry", s.handleRetryJob)
	s.mux.HandleFunc("DELETE /api/v1/jobs/{id}", s.handleDeleteJob)
	s.mux.Handle("GET /api/v1/metrics", promhttp.Handler())
	s.mux.HandleFunc("GET /", s.handleIndex)
}

// ServeHTTP makes Server itself usable directly with http.ListenAndServe.
func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	s.mux.ServeHTTP(w, r)
}
