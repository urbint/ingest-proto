package ingest

// passthrough is an empty runner that will emit records to a channel
type passthrough struct {
	name    string
	out     chan interface{}
	noClose bool
}

func newPassthrough(name string, out chan interface{}, noClose bool) *passthrough {
	return &passthrough{name, out, noClose}
}

// Name implements the Runner interfacce for passthrough
func (p *passthrough) Name() string {
	return p.name
}

// Run implements the Runner interface for passthrough
func (p *passthrough) Run(stage *Stage) error {
	defer func() {
		if !p.noClose {
			close(p.out)
		}
	}()

	for {
		select {
		case <-stage.Abort:
			return nil
		case rec, ok := <-stage.In:
			if !ok {
				return nil
			}
			select {
			case <-stage.Abort:
				return nil
			case p.out <- rec:
				if stage.Out != nil {
					select {
					case <-stage.Abort:
						return nil
					case stage.Out <- rec:
						continue
					}
				}
			}
		}
	}
}

// SkipAbortErr saves us having to send nil errors back on abort
func (p *passthrough) SkipAbortErr() bool {
	return true
}

// PassOnAddTarget makes it so that a passthrough stream is not targeted
func (p *passthrough) PassOnAddTarget() bool {
	return true
}
