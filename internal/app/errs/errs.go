package errs

import "errors"

type Invalid struct{ Reason string }

func (e *Invalid) Error() string { return e.Reason }

type Forbidden struct{ Reason string }

func (e *Forbidden) Error() string { return e.Reason }

type Conflict struct{ Reason string }

func (e *Conflict) Error() string { return e.Reason }

type NotFound struct{ Reason string }

func (e *NotFound) Error() string { return e.Reason }

type Unavailable struct{ Reason string }

func (e *Unavailable) Error() string { return e.Reason }

// Permanent is a poison message or a request that will not succeed on retry.
type Permanent struct{ Err error }

func (e *Permanent) Error() string {
	if e.Err == nil {
		return "permanent failure"
	}
	return e.Err.Error()
}

func (e *Permanent) Unwrap() error { return e.Err }

func IsPermanent(err error) bool {
	var inv *Invalid
	var conf *Conflict
	var forb *Forbidden
	var perm *Permanent
	return errors.As(err, &inv) || errors.As(err, &conf) || errors.As(err, &forb) || errors.As(err, &perm)
}
