package observe

import "time"

type Recorder interface {
	Submit(kind, status string, elapsed time.Duration)
	Replay()
	Conflict()
	Retry(component string)
	DLQ()
	Divergence()
	OutboxLag(seconds float64)
}

type Nop struct{}

func (Nop) Submit(string, string, time.Duration) {}
func (Nop) Replay()                              {}
func (Nop) Conflict()                            {}
func (Nop) Retry(string)                         {}
func (Nop) DLQ()                                 {}
func (Nop) Divergence()                          {}
func (Nop) OutboxLag(float64)                    {}
