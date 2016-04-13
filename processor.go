package ingest

// A Processor is a signle unit in an ingest Pipeline.
//
// It receives inputs on the In channel and is expected to emit records
// on the Out channel. Errors can be reported on the Err channel
//
// It also includes a Quit channel which, when closed should cause the
// processor to abort reasonably soon there after.
type Processor struct {
	In    chan interface{}
	Out   chan interface{}
	Err   chan error
	Abort chan bool
}

// GetProcessor is an accessor function that returns the Processor.
//
// It is useful for embedding a Processor inside of another struct
func (p Processor) GetProcessor() Processor {
	return p
}

// Start is a placeholder method that a Struct embedding a processor needs
// to overwrite. Panics by default
func (p Processor) Start() {
	panic("No start method implemented!")
}
