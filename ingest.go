package ingest

// StartWith will build a pipeline that emits the provided argument as a first step
func StartWith(data interface{}) *Pipeline {
	in := make(chan interface{})
	go func() {
		in <- data
		close(in)
	}()
	return NewPipeline().Then(NewInStream("Start", in))
}

// StreamFrom will build a pipeline that reads events from the provided channel
func StreamFrom(input chan interface{}) *Pipeline {
	return NewPipeline().Then(NewInStream("Start", input))
}
