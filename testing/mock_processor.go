package ingesttesting

import (
	"github.com/urbint/ingest"
	"time"
)

// MockProcessor is an ingest.Runner that exposes interfaces useful for testing
type MockProcessor struct {
	Opts          MockOpt
	Started       bool
	RunCalledWith *ingest.Stage
}

// MockOpt is used to configure the MockProcessor
type MockOpt struct {
	Err  error
	Wait time.Duration
	Out  []interface{}
}

// NewMockProcessor creates a new MockProcessor with the specified options
func NewMockProcessor(opts ...MockOpt) *MockProcessor {
	var opt MockOpt
	if len(opts) == 0 {
		opt = MockOpt{}
	} else {
		opt = opts[0]
	}
	return &MockProcessor{
		Opts: opt,
	}
}

// Run implements ingest.Runnable for MockProcessor
func (m *MockProcessor) Run(stage *ingest.Stage) error {
	m.Started = true
	m.RunCalledWith = stage
	time.Sleep(m.Opts.Wait)

	for _, item := range m.Opts.Out {
		stage.Send(item)
		time.Sleep(m.Opts.Wait)
	}
	return m.Opts.Err
}
