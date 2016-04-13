package ingest

// Open is a shortcut to create a new Pipeline that starts with the
// specified path or file.
//
// If the path is a directory, files can be selected from the directory using ingest.Select
// If the path is a file, the file will be emitted to the next Processor
func Open(path string) Pipeline {
	return NewPipeline()
}

// Select is a processor used to grab a subset of data from the previous stage.
//
// Select will panic if it is used after a non-selectable Processor
func Select(selection ...string) *Processor {
	return &Processor{}
}
