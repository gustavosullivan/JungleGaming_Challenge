package config

import (
	"fmt"
	"os"
	"strconv"
	"time"
)

type Config struct {
	HTTPAddr             string
	DatabaseURL          string
	SQSEndpoint          string
	SQSRegion            string
	AWSAccessKey         string
	AWSSecretKey         string
	QueueURL             string
	DLQURL               string
	EventsQueueURL       string
	OIDCIssuer           string
	OIDCJWKSURL          string
	OIDCAudience         string
	ReferenceMaxAttempts int
	ReferenceTTL         time.Duration
	ReferenceBackoff     time.Duration
	ReferencePoll        time.Duration
	OutboxPoll           time.Duration
	OutboxLease          time.Duration
	SQSWaitSeconds       int32
	SQSVisibilitySeconds int32
	SQSMaxMessages       int32
	ShutdownTimeout      time.Duration
}

func Load() (Config, error) {
	cfg := Config{
		HTTPAddr:             env("HTTP_ADDR", ":8080"),
		DatabaseURL:          os.Getenv("DATABASE_URL"),
		SQSEndpoint:          os.Getenv("SQS_ENDPOINT"),
		SQSRegion:            env("SQS_REGION", "us-east-1"),
		AWSAccessKey:         env("AWS_ACCESS_KEY_ID", "test"),
		AWSSecretKey:         env("AWS_SECRET_ACCESS_KEY", "test"),
		QueueURL:             os.Getenv("SQS_QUEUE_URL"),
		DLQURL:               os.Getenv("SQS_DLQ_URL"),
		EventsQueueURL:       os.Getenv("SQS_EVENTS_QUEUE_URL"),
		OIDCIssuer:           os.Getenv("OIDC_ISSUER"),
		OIDCJWKSURL:          os.Getenv("OIDC_JWKS_URL"),
		OIDCAudience:         env("OIDC_AUDIENCE", "wagering-api"),
		ReferenceMaxAttempts: envInt("REFERENCE_MAX_ATTEMPTS", 8),
		ReferenceTTL:         envDuration("REFERENCE_TTL", 2*time.Minute),
		ReferenceBackoff:     envDuration("REFERENCE_BACKOFF", 200*time.Millisecond),
		ReferencePoll:        envDuration("REFERENCE_POLL_INTERVAL", 500*time.Millisecond),
		OutboxPoll:           envDuration("OUTBOX_POLL_INTERVAL", 500*time.Millisecond),
		OutboxLease:          envDuration("OUTBOX_LEASE", 30*time.Second),
		SQSWaitSeconds:       int32(envInt("SQS_WAIT_TIME_SECONDS", 5)),
		SQSVisibilitySeconds: int32(envInt("SQS_VISIBILITY_TIMEOUT", 30)),
		SQSMaxMessages:       int32(envInt("SQS_MAX_MESSAGES", 5)),
		ShutdownTimeout:      envDuration("SHUTDOWN_TIMEOUT", 20*time.Second),
	}
	if cfg.ReferenceMaxAttempts < 1 {
		return Config{}, fmt.Errorf("REFERENCE_MAX_ATTEMPTS must be >= 1")
	}
	required := []struct {
		name, value string
	}{
		{"DATABASE_URL", cfg.DatabaseURL},
		{"SQS_ENDPOINT", cfg.SQSEndpoint},
		{"SQS_QUEUE_URL", cfg.QueueURL},
		{"SQS_DLQ_URL", cfg.DLQURL},
		{"SQS_EVENTS_QUEUE_URL", cfg.EventsQueueURL},
		{"OIDC_ISSUER", cfg.OIDCIssuer},
		{"OIDC_JWKS_URL", cfg.OIDCJWKSURL},
	}
	for _, item := range required {
		if item.value == "" {
			return Config{}, fmt.Errorf("%s is required", item.name)
		}
	}
	return cfg, nil
}

func env(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func envInt(key string, fallback int) int {
	v := os.Getenv(key)
	if v == "" {
		return fallback
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		return fallback
	}
	return n
}

func envDuration(key string, fallback time.Duration) time.Duration {
	v := os.Getenv(key)
	if v == "" {
		return fallback
	}
	d, err := time.ParseDuration(v)
	if err != nil {
		return fallback
	}
	return d
}
