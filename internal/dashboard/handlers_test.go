package dashboard

import (
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"sidekiq"
	"sidekiq/internal/broker"
)

func mustJob(t *testing.T, queue, jobType string, status sidekiq.JobStatus) *sidekiq.Job {
	t.Helper()
	job, err := sidekiq.NewJob(queue, jobType, map[string]string{"k": "v"})
	if err != nil {
		t.Fatalf("NewJob failed: %v", err)
	}
	job.Status = status
	return job
}

func TestHandleGetQueues(t *testing.T) {
	fb := newFakeBroker()
	fb.stats["critical"] = broker.QueueStats{Queue: "critical", Enqueued: 3, Dead: 1}
	s := NewServer(fb)

	req := httptest.NewRequest(http.MethodGet, "/api/v1/queues", nil)
	rec := httptest.NewRecorder()
	s.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}
	var got map[string]broker.QueueStats
	if err := json.Unmarshal(rec.Body.Bytes(), &got); err != nil {
		t.Fatalf("unmarshal response: %v", err)
	}
	if got["critical"].Enqueued != 3 {
		t.Errorf("Enqueued = %d, want 3", got["critical"].Enqueued)
	}
}

func TestHandleGetQueuesPropagatesBrokerError(t *testing.T) {
	fb := newFakeBroker()
	fb.getQueuesErr = errors.New("boom")
	s := NewServer(fb)

	req := httptest.NewRequest(http.MethodGet, "/api/v1/queues", nil)
	rec := httptest.NewRecorder()
	s.ServeHTTP(rec, req)

	if rec.Code != http.StatusInternalServerError {
		t.Errorf("status = %d, want 500", rec.Code)
	}
}

func TestHandleListJobsDefaultsToPending(t *testing.T) {
	fb := newFakeBroker()
	job := mustJob(t, "default", "T", sidekiq.StatusPending)
	fb.jobs[job.ID] = job
	s := NewServer(fb)

	req := httptest.NewRequest(http.MethodGet, "/api/v1/jobs", nil)
	rec := httptest.NewRecorder()
	s.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}
	var got []*sidekiq.Job
	if err := json.Unmarshal(rec.Body.Bytes(), &got); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if len(got) != 1 || got[0].ID != job.ID {
		t.Errorf("got %+v, want [%s]", got, job.ID)
	}
}

func TestHandleListJobsEmptyReturnsEmptyArrayNotNull(t *testing.T) {
	fb := newFakeBroker()
	s := NewServer(fb)

	req := httptest.NewRequest(http.MethodGet, "/api/v1/jobs?status=dead", nil)
	rec := httptest.NewRecorder()
	s.ServeHTTP(rec, req)

	if rec.Body.String() != "[]\n" {
		t.Errorf("body = %q, want %q (empty array, not null)", rec.Body.String(), "[]\n")
	}
}

func TestHandleGetJobFound(t *testing.T) {
	fb := newFakeBroker()
	job := mustJob(t, "default", "T", sidekiq.StatusPending)
	fb.jobs[job.ID] = job
	s := NewServer(fb)

	req := httptest.NewRequest(http.MethodGet, "/api/v1/jobs/"+job.ID, nil)
	rec := httptest.NewRecorder()
	s.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}
}

func TestHandleGetJobNotFound(t *testing.T) {
	fb := newFakeBroker()
	s := NewServer(fb)

	req := httptest.NewRequest(http.MethodGet, "/api/v1/jobs/nonexistent", nil)
	rec := httptest.NewRecorder()
	s.ServeHTTP(rec, req)

	if rec.Code != http.StatusNotFound {
		t.Errorf("status = %d, want 404", rec.Code)
	}
}

func TestHandleRetryJob(t *testing.T) {
	fb := newFakeBroker()
	job := mustJob(t, "default", "T", sidekiq.StatusDead)
	fb.jobs[job.ID] = job
	s := NewServer(fb)

	req := httptest.NewRequest(http.MethodPost, "/api/v1/jobs/"+job.ID+"/retry", nil)
	rec := httptest.NewRecorder()
	s.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}
	if len(fb.retriedIDs) != 1 || fb.retriedIDs[0] != job.ID {
		t.Errorf("retriedIDs = %v, want [%s]", fb.retriedIDs, job.ID)
	}
}

func TestHandleRetryJobPropagatesError(t *testing.T) {
	fb := newFakeBroker()
	fb.retryErr = errors.New("not dead")
	s := NewServer(fb)

	req := httptest.NewRequest(http.MethodPost, "/api/v1/jobs/some-id/retry", nil)
	rec := httptest.NewRecorder()
	s.ServeHTTP(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Errorf("status = %d, want 400", rec.Code)
	}
}

func TestHandleDeleteJob(t *testing.T) {
	fb := newFakeBroker()
	job := mustJob(t, "default", "T", sidekiq.StatusDead)
	fb.jobs[job.ID] = job
	s := NewServer(fb)

	req := httptest.NewRequest(http.MethodDelete, "/api/v1/jobs/"+job.ID, nil)
	rec := httptest.NewRecorder()
	s.ServeHTTP(rec, req)

	if rec.Code != http.StatusNoContent {
		t.Fatalf("status = %d, want 204", rec.Code)
	}
	if len(fb.deletedIDs) != 1 || fb.deletedIDs[0] != job.ID {
		t.Errorf("deletedIDs = %v, want [%s]", fb.deletedIDs, job.ID)
	}
}

func TestHandleIndexServesHTML(t *testing.T) {
	fb := newFakeBroker()
	s := NewServer(fb)

	req := httptest.NewRequest(http.MethodGet, "/", nil)
	rec := httptest.NewRecorder()
	s.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}
	ct := rec.Header().Get("Content-Type")
	if ct != "text/html; charset=utf-8" {
		t.Errorf("Content-Type = %q, want text/html", ct)
	}
	if len(rec.Body.Bytes()) == 0 {
		t.Error("expected non-empty HTML body")
	}
}

func TestHandleMetricsIsServed(t *testing.T) {
	fb := newFakeBroker()
	s := NewServer(fb)

	req := httptest.NewRequest(http.MethodGet, "/api/v1/metrics", nil)
	rec := httptest.NewRecorder()
	s.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}
}
