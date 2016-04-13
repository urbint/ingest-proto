package ingest

// A Pipeline is a sequence of ingest.Processors that are linked together
//
// The processors will share common control channels (Err, Quit) which are managed
// via the ingest.Job (which is created by calling Build or Run on the Pipeline).
type Pipeline struct {
}

// Processable is an interface which can be processed by a pipeline
type Processable interface {
	GetProcessor() Processor
	Start()
}

// NewPipeline instantiates a new pipeline for use
func NewPipeline() Pipeline {
	return Pipeline{}
}

// Then queues the provided processor to opperate on the data emitted by the previous
// stage.
//
// Then returns a new pipeline with the aforemention stage added
func (p Pipeline) Then(stage Processable) Pipeline {
	return p
}

// Build builds the pipeline and returns a Job control structure
func (p Pipeline) Build() *Job {
	return &Job{}
}
