package ingest

// A Job is a control structure for interacting with a Pipeline
type Job struct {
}

// Run runs the Job and blocks until it has completed
//
// It returns any error that occured anywhere in the pipeline
func (j *Job) Run() error {
	return nil
}

// Start starts the job but does not block
//
// It returns itself for a chainable API
func (j *Job) Start() *Job {
	return j
}

// Error returns any error that the Job encountered while running
func (j *Job) Error() error {
	return nil
}

// Wait waits for the job to complete and returns any error encountered
func (j *Job) Wait() error {
	return nil
}
