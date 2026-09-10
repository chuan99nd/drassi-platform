package planner

import "errors"

// ErrInvalidWorkflow wraps every error Plan returns because the supplied
// workflow YAML failed pkg/schema validation, failed to parse into an act
// model.Workflow, or failed to plan (e.g. an unresolvable needs graph).
// Callers (T-M1-05's HTTP handler) can use errors.Is(err, ErrInvalidWorkflow)
// to map this to HTTP 400 instead of 500.
var ErrInvalidWorkflow = errors.New("planner: invalid workflow")
