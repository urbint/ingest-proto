package ingest

// A Pipeline is a sequence of ingest.Processors that are linked together
//
// The processors will share common control channels (Err, Quit) which are managed
// via the ingest.Job (which is created by calling Build or Run on the Pipeline).
type Pipeline struct {
	configs []runnerConfig
}

type runnerConfig struct {
	Runner Runner
	Opts   ThenOpts
}

// ThenOpts are an optional argument to configure the behavior of calling Then
type ThenOpts struct {
	// InBuffer is the size of the buffer for the input channel
	InBuffer int
	// OutBuffer is the size of the buffer for the output channel
	OutBuffer int
}

// NewPipeline instantiates a new pipeline for use
func NewPipeline() *Pipeline {
	return &Pipeline{}
}

// Then queues the provided processor to opperate on the data emitted by the previous
// stage.
//
// Then returns the pipeline for a chainable API
func (p *Pipeline) Then(runner Runner, opts ...ThenOpts) *Pipeline {
	var opt ThenOpts

	if len(opts) == 0 {
		if asOptionMaker, hasDefaults := runner.(HasDefaultOptions); hasDefaults {
			opt = asOptionMaker.DefaultOpts()
		} else {
			opt = ThenOpts{}
		}
	} else {
		opt = opts[0]
	}

	if asOnAdd, hasOnAdd := runner.(OnAdd); hasOnAdd {
		var prevRunner Runner
		if len(p.configs) != 0 {
			prevRunner = p.configs[len(p.configs)-1].Runner
		}
		asOnAdd.OnAdd(prevRunner)
	}

	// If it's a noop, skip adding it to the config / pipeline
	if asNoOp, isNoOp := runner.(NoOpRunner); isNoOp && asNoOp.NoOpRunner() {
		return p
	}

	p.configs = append(p.configs, runnerConfig{runner, opt})

	return p
}

// StreamTo causes the pipeline emit records at that stage a channel passed as an argument
//
// An optional name can be specified as a string argument
func (p *Pipeline) StreamTo(out chan interface{}, nameArg ...string) *Pipeline {
	var name string
	if len(nameArg) == 0 {
		name = "Unknown"
	} else {
		name = nameArg[0]
	}
	return p.Then(newPassthrough(name, out))
}

// Build builds the pipeline and returns a Job control structure
func (p *Pipeline) Build() *Job {
	return NewJob(*p)
}
