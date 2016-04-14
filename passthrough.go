package ingest

// passthrough is an empty runner that will emit records to a channel
type passthrough struct {
	out chan interface{}
}

func newPassthrough(out chan interface{}) *passthrough {
	return &passthrough{out}
}

// Run implements Runnable for the passthrough
func (p *passthrough) Run(stage *Stage) error {
	for rec := range stage.In {
		p.out <- rec
	}
	close(p.out)
	return nil
}
