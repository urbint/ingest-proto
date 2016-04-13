package ingest

import (
	"sync"
)

// A Job is a control structure for interacting with a Pipeline
type Job struct {
	pipeline *Pipeline

	runnerCount int

	abortChan chan chan<- error

	err error
	mu  sync.Mutex
	wg  sync.WaitGroup
}

// NewJob builds a job with the specified pipeline
func NewJob(pipeline Pipeline) *Job {

	runnerCount := len(pipeline.configs)

	return &Job{
		pipeline:    &pipeline,
		runnerCount: runnerCount,
		abortChan:   make(chan chan<- error, runnerCount),
		mu:          sync.Mutex{},
	}
}

// Run runs the Job and blocks until it has completed
//
// It returns any error that occured anywhere in the pipeline
func (j *Job) Run() error {
	j.Start()
	j.wg.Wait()
	return j.Error()
}

// Start starts the job but does not block
//
// It returns itself for a chainable API
func (j *Job) Start() *Job {
	configs := j.pipeline.configs
	j.mu.Lock()
	defer j.mu.Unlock()

	j.wg = sync.WaitGroup{}
	j.wg.Add(j.runnerCount)

	for i := range configs {
		go func(i int) {
			config := configs[i]
			var prevRunner, nextRunner Runner

			if i != 0 {
				prevRunner = configs[i-1].Runner
			}
			if i != len(configs)-1 {
				nextRunner = configs[i+1].Runner
			}
			defer j.wg.Done()
			err := config.Runner.Run(&Stage{
				IsFirst:    i == 0,
				IsLast:     i == len(configs)-1,
				NextRunner: nextRunner,
				PrevRunner: prevRunner,
				In:         make(chan interface{}),

				out: make(chan interface{}),
				job: j,
			})

			j.handleError(err)
		}(i)
	}

	return j
}

// Error returns any error that the Job encountered while running
func (j *Job) Error() error {
	j.mu.Lock()
	defer j.mu.Unlock()
	return j.err
}

// Wait waits for the job to complete and returns any error encountered
func (j *Job) Wait() error {
	j.wg.Wait()
	return j.Error()
}

// Abort aborts the job and cancels all running processors
//
// It returns any error encountered while aborting the job
func (j *Job) Abort() error {
	return nil
}

// handleError handles an error if it exists.
// It will store the error so it can be returned via j.Error()
// If an error is already stored, the new error will be discarded
func (j *Job) handleError(err error) {
	if err == nil {
		return
	}

	j.mu.Lock()
	defer j.mu.Unlock()
	j.err = err
}
