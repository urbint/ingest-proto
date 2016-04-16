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
		prevRunner := p.lastTargetableRunner()
		if prevRunner != nil {
			asOnAdd.OnAdd(prevRunner)
		}
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
func (p *Pipeline) StreamTo(out chan interface{}, opt ...StreamToOpt) *Pipeline {
	var name string
	var noClose bool
	if len(opt) == 0 || opt[0].Name == "" {
		name = "Unknown"
	} else if len(opt) != 0 {
		name = opt[0].Name
		noClose = opt[0].NoClose
	}

	return p.Then(newPassthrough(name, out, noClose))
}

// StreamToOpt are optional options to StreamTo
type StreamToOpt struct {
	// Name is used for debugging and logging
	Name string
	// NoClose keeps the out channel open even once the stage before it has finished
	NoClose bool
}

// ForEach runs a transform function on each record in the pipeline.
//
// The record returned will be forwarded to the later stages
// Returning an error will cause the pipeline to fail
// An optional name can be specified as a string argument and will be used for logging
func (p *Pipeline) ForEach(fn TransformFn, nameArg ...string) *Pipeline {
	var name string
	if len(nameArg) == 0 {
		name = "Unknown"
	} else {
		name = nameArg[0]
	}
	return p.Then(NewTransformStream(name, fn))
}

// Build builds the pipeline and returns a Job control structure
func (p *Pipeline) Build() *Job {
	return NewJob(*p)
}

// lastTargetableRunner returns the last runner that can be targeted (ie. does not implement PassOnAddTarget)
func (p *Pipeline) lastTargetableRunner() Runner {
	if len(p.configs) == 0 {
		return nil
	}

	// Walk through the configs backwards until we find a targetable config
	for i := len(p.configs) - 1; i >= 0; i-- {
		if asPass, dynamicTarget := p.configs[i].Runner.(PassOnAddTarget); dynamicTarget && asPass.PassOnAddTarget() {
			continue
		}
		return p.configs[i].Runner
	}
	return nil
}
