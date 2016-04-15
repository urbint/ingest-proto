package ingest

// An InStream is a runner that will pass data from the input channel down to future runners
type InStream struct {
	in   chan interface{}
	name string
}

// NewInStream builds an input stream for the specified input channel
func NewInStream(name string, in chan interface{}) *InStream {
	return &InStream{in, name}
}

// Name implements Runner for InStream
func (i *InStream) Name() string {
	return i.name
}

// Run implements Runner for InStream
func (i *InStream) Run(stage *Stage) error {
	for {
		select {
		case <-stage.Abort:
			return nil
		case rec, ok := <-i.in:
			if !ok {
				return nil
			}
			select {
			case <-stage.Abort:
				return nil
			case stage.Out <- rec:
				continue
			}
		}
	}
}

// SkipAbortErr saves us having to send nil errors back on abort
func (i *InStream) SkipAbortErr() bool {
	return true
}
