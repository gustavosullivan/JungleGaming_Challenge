package metrics

import (
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

type Recorder struct {
	submit     *prometheus.HistogramVec
	replays    prometheus.Counter
	conflicts  prometheus.Counter
	retries    *prometheus.CounterVec
	dlq        prometheus.Counter
	divergence prometheus.Counter
	outboxLag  prometheus.Gauge
}

func New() *Recorder {
	r := &Recorder{
		submit: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "wager_processing_seconds",
			Help:    "Time spent applying a wager command, labeled by kind and resulting status.",
			Buckets: prometheus.DefBuckets,
		}, []string{"kind", "status"}),
		replays: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "wager_idempotent_replays_total",
			Help: "Wager commands answered from a stored result.",
		}),
		conflicts: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "wager_concurrency_conflicts_total",
			Help: "Optimistic version mismatches and similar write races.",
		}),
		retries: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "wager_retries_total",
			Help: "Retries by component.",
		}, []string{"component"}),
		dlq: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "wager_dlq_total",
			Help: "Inbound messages classified as permanent failures.",
		}),
		divergence: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "wallet_reconciliation_divergences_total",
			Help: "Reconciliations whose stored balance did not match the ledger.",
		}),
		outboxLag: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "outbox_oldest_unpublished_seconds",
			Help: "Age of the oldest outbox row that has not been published.",
		}),
	}
	return r
}

func (r *Recorder) Register(reg prometheus.Registerer) error {
	for _, c := range []prometheus.Collector{r.submit, r.replays, r.conflicts, r.retries, r.dlq, r.divergence, r.outboxLag} {
		if err := reg.Register(c); err != nil {
			return err
		}
	}
	return nil
}

func (r *Recorder) Submit(kind, status string, elapsed time.Duration) {
	r.submit.WithLabelValues(kind, status).Observe(elapsed.Seconds())
}
func (r *Recorder) Replay()                { r.replays.Inc() }
func (r *Recorder) Conflict()              { r.conflicts.Inc() }
func (r *Recorder) Retry(component string) { r.retries.WithLabelValues(component).Inc() }
func (r *Recorder) DLQ()                   { r.dlq.Inc() }
func (r *Recorder) Divergence()            { r.divergence.Inc() }
func (r *Recorder) OutboxLag(seconds float64) {
	r.outboxLag.Set(seconds)
}
