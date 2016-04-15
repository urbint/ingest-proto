package ingest

import (
	"fmt"
	"sync"
	"time"
)

// AbortTimeout is the duration after which aborting will be assumed as timed out. It will be logged as a warning
var AbortTimeout = time.Second * 10

// A Job is a control structure for interacting with a Pipeline
type Job struct {
	pipeline *Pipeline

	runnerCount int
	running     []*runState

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
		running:     []*runState{},
		mu:          sync.Mutex{},
	}
}

type runState struct {
	Runner    Runner
	AbortChan chan chan error
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
		j.reset()
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

		run := j.newRunState(configs[i].Runner)

		// Start each worker
		go func(i int, in chan interface{}, out chan interface{}, abort chan chan error) {
			config := configs[i]

			defer func() {
				stages.Done()
				if out != nil {
					close(out)
				}
			}()

			err := config.Runner.Run(&Stage{
				Abort: abort,
				In:    in,
				Out:   out,
			})

			if err != nil {
				DefaultLogger.WithError(err).Error("Error while running pipeline")
			}

			j.handleError(err)
		}(i, in, out, run.AbortChan)
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
// It returns a channel of errors encountered while aborting
func (j *Job) Abort() <-chan error {
	result := make(chan error, j.runnerCount)
	wg := sync.WaitGroup{}
	wg.Add(j.runnerCount)

	go func() {
		wg.Wait()
		close(result)
	}()

	for i := 0; i < j.runnerCount; i++ {
		go func(index int) {
			defer wg.Done()

			run := j.running[index]
			abortDone := make(chan error)
			defer DefaultLogger.Debug(fmt.Sprintf(`Aborted stage "%s"`, run.Runner.Name()))

			select {
			case <-time.After(AbortTimeout):
				DefaultLogger.Warn(fmt.Sprintf(`Sending abort signal for stage "%s" timed out. It doesn't seem to be abortable`, run.Runner.Name()))
				result <- fmt.Errorf("Stage %s timed out while sending abort", run.Runner.Name())
			case run.AbortChan <- abortDone:
				if asNoAbort, isNoErrAbortRunner := run.Runner.(NoErrAbortRunner); isNoErrAbortRunner && asNoAbort.SkipAbortErr() {
					result <- nil
					return
				}

				select {
				case <-time.After(AbortTimeout):
					DefaultLogger.Warn(fmt.Sprintf(`Aborting stage "%s" timed out`, run.Runner.Name()))
					result <- fmt.Errorf("Stage %s timed out while aborting", run.Runner.Name())
				case err := <-abortDone:
					result <- err
				}
			}
		}(i)
	}
	return result
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
	if j.err == nil {
		j.err = err
	}
}

// reset resets the jobs run states, etc.
func (j *Job) reset() {
	j.running = []*runState{}
}

// newRunState builds a *runState and appends it to j.runners
func (j *Job) newRunState(runner Runner) *runState {
	runState := &runState{
		Runner:    runner,
		AbortChan: make(chan chan error),
	}
	j.running = append(j.running, runState)
	return runState
}
