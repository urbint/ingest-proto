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

// RunAsync runs the job and returns an error channel that will emit the result
// of the job and then be closed
func (j *Job) RunAsync() <-chan error {
	result := make(chan error)
	go func() {
		result <- j.Run()
		close(result)
	}()
	return result
}

// Start starts the job but does not block
//
// It returns itself for a chainable API
func (j *Job) Start() *Job {
	configs := j.pipeline.configs
	j.mu.Lock()
	defer j.mu.Unlock()

	j.wg = sync.WaitGroup{}

	stages := sync.WaitGroup{}
	stages.Add(j.runnerCount)
	j.wg.Add(2) // Add one for all runners finishing and one for cleanup

	go func() {
		stages.Wait()
		j.wg.Done()
	}()

	var in chan interface{}
	var out chan interface{}

	for i := range configs {
		isLast := i == len(configs)-1

		in = out
		if !isLast {
			out = make(chan interface{})
		} else {
			out = nil
		}

		// Start each worker
		go func(i int, in chan interface{}, out chan interface{}) {
			config := configs[i]

			defer func() {
				stages.Done()
				if out != nil {
					close(out)
				}
			}()

			err := config.Runner.Run(&Stage{
				In:  in,
				Out: out,
			})

			j.handleError(err)
		}(i, in, out)
	}

	// Wait for all stages to complete and run OnDone for the ones that define it
	go func() {
		stages.Wait()
		defer j.wg.Done()
		for _, config := range configs {
			if asDone, hasDone := config.Runner.(OnDone); hasDone {
				err := asDone.OnPipelineDone()
				j.handleError(err)
			}
		}
	}()

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
