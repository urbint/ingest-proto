package ingest

import "time"

type MockProcessor struct {
	Opts          MockOpt
	Started       bool
	RunCalledWith *Stage
	Aborted       bool
}

type MockOpt struct {
	Err  error
	Wait time.Duration
	Out  []interface{}
}

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

func (m *MockProcessor) Name() string {
	return "Mock processor"
}

func (m *MockProcessor) Run(stage *Stage) error {
	m.Started = true
	m.RunCalledWith = stage
	select {
	case <-time.After(m.Opts.Wait):
	case errChan := <-stage.Abort:
		m.Aborted = true
		errChan <- m.Opts.Err
		return nil
	}

	for _, item := range m.Opts.Out {
		select {
		case errChan := <-stage.Abort:
			m.Aborted = true
			errChan <- m.Opts.Err
			return nil
		case stage.Out <- item:
		}
		time.Sleep(m.Opts.Wait)
	}
	return m.Opts.Err
}
