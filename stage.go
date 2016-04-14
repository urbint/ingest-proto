package ingest

// A Stage is a control structure passed to an induvidual Runner
type Stage struct {
	In    chan interface{}
	Out   chan interface{}
	Abort <-chan chan error
}

// NewStage builds a blank Stage.
//
// It is mostly to facilitate testing and rarely called directly
func NewStage() *Stage {
	return &Stage{
		In:    make(chan interface{}),
		Out:   make(chan interface{}),
		Abort: make(chan chan error),
	}
}

// Send sends an item to the next stage, if there is one. Otherwise, it does nothing
func (s *Stage) Send(item interface{}) {
	if s.Out != nil {
		s.Out <- item
	}
}
