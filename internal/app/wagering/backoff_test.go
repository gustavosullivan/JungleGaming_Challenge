package wagering

import (
	"testing"
	"time"
)

func TestBackoff(t *testing.T) {
	base := 200 * time.Millisecond
	cap := time.Second
	if got := backoff(1, base, cap); got != base {
		t.Fatalf("attempt 1 = %s", got)
	}
	if got := backoff(2, base, cap); got != 400*time.Millisecond {
		t.Fatalf("attempt 2 = %s", got)
	}
	if got := backoff(10, base, cap); got != cap {
		t.Fatalf("capped = %s", got)
	}
}
