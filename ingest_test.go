package ingest

import "time"

type MockProcessor struct {
	Opts          MockOpt
	Started       bool
	RunCalledWith *Stage
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

func (m *MockProcessor) Run(stage *Stage) error {
	m.Started = true
	m.RunCalledWith = stage
	time.Sleep(m.Opts.Wait)

	for _, item := range m.Opts.Out {
		stage.Send(item)
		time.Sleep(m.Opts.Wait)
	}
	return m.Opts.Err
}
