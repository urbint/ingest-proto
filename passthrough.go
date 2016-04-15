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
	defer func() {
		close(p.out)
	}()

	for rec := range stage.In {
		select {
		case errChan := <-stage.Abort:
			errChan <- nil
		case p.out <- rec:
		}
	}
	return nil
}
