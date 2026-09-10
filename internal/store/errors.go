package store

import "errors"

// ErrIllegalTransition is returned by JobStore.TransitionStatus (and Lease,
// which is itself a guarded transition) when the requested from->to move is
// not allowed by the job status state machine, or when the row's current
// status no longer matches from (a lost race against another caller).
var ErrIllegalTransition = errors.New("store: illegal job status transition")
