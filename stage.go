package ingest

// A Stage is a control structure passed to an induvidual Runner
type Stage struct {
	NextRunner Runner
	PrevRunner Runner
	IsFirst    bool
	IsLast     bool
	In         <-chan interface{}

	out chan<- interface{}
	job *Job
}

// Send sends an item to the next stage, if there is one. Otherwise, it does nothing
func (s *Stage) Send(item interface{}) {
	if !s.IsLast {
		s.out <- item
	}
}
