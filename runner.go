package ingest

// Runner is an interface which can be processed by a pipeline
type Runner interface {
	Name() string
	Run(*Stage) error
}

// HasDefaultOptions is an interface which a Runner can implement
// that allows it to specify default ThenOpts
type HasDefaultOptions interface {
	Runner
	DefaultOpts() ThenOpts
}

// OnAdd is an interface which a Runner can implement
// to allow it to hook in to being added to the pipeline
type OnAdd interface {
	Runner
	OnAdd(prevRunner Runner)
}

// OnDone is an interface which a Runner can implement
// to allow it to run code after the pipeline has completed running fully
type OnDone interface {
	Runner
	OnPipelineDone() error
}

// NoOpRunner allows a runner to specify that it shouldn't be added
// to the run pipeline at add time
type NoOpRunner interface {
	Runner
	NoOpRunner() bool
}

// NoErrAbortRunner allows a runner to avoid having to send an error
// back on Abort
type NoErrAbortRunner interface {
	Runner
	SkipAbortErr() bool
}
