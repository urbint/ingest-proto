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

// A TransformStream is a stream which will run a transform function
// on all entries before emitting them back out
type TransformStream struct {
	name        string
	transformFn TransformFn
}

// A TransformFn is the function signature used by the TransformStream
type TransformFn func(rec interface{}) (res interface{}, err error)

// NewTransformStream builds an input stream for the specified input channel
func NewTransformStream(name string, transformFn TransformFn) *TransformStream {
	return &TransformStream{
		name:        name,
		transformFn: transformFn,
	}
}

// Name implements Runner for TransformStream
func (t *TransformStream) Name() string {
	return t.name
}

// Run implements Runner for TransformStream
func (t *TransformStream) Run(stage *Stage) error {
	for {
		select {
		case <-stage.Abort:
			return nil
		case rec, ok := <-stage.In:
			if !ok {
				return nil
			}
			rec, err := t.transformFn(rec)
			if err != nil {
				return err
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
func (t *TransformStream) SkipAbortErr() bool {
	return true
}
