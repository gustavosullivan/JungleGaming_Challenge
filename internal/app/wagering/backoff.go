package wagering

import "time"

func backoff(attempt int, base, cap time.Duration) time.Duration {
	if attempt < 1 {
		attempt = 1
	}
	d := base
	for i := 1; i < attempt; i++ {
		if d >= cap/2 {
			return cap
		}
		d *= 2
	}
	if d > cap {
		return cap
	}
	return d
}
