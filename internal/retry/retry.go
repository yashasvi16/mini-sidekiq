package retry

import (
	"math"
	"math/rand"
	"time"
)

// NextDelay returns how long to wait before retrying a failed job, using
// Sidekiq's own backoff formula: attempts^4 + 15 seconds, plus jitter so
// many simultaneously-failing jobs don't all retry at the exact same instant.
// attempt=1 -> ~15-45s, attempt=2 -> ~31-91s, attempt=5 -> ~10-13min, growing
// fast thereafter.
func NextDelay(attempt int) time.Duration {
	base := math.Pow(float64(attempt), 4) + 15
	jitter := float64(rand.Intn(30)) * float64(attempt+1)
	seconds := base + jitter
	return time.Duration(seconds) * time.Second
}
