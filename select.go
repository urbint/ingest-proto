package ingest

import "fmt"

// Selectable is an interface that a Runner can implement
// which allows it to be filtered via the SetSelection method
type Selectable interface {
	Runner
	SetSelection(selection ...string)
}

// Select builds a NoOp Selector runner that will configure the previous stage
func Select(selection ...string) *Selector {
	return &Selector{selection}
}

// Selector is a NoOp runner that will configure the previous stage
type Selector struct {
	selection []string
}

// Name implements Runner for Selector
func (s *Selector) Name() string {
	return "Select"
}

// Run implements Runner for Selector
func (s *Selector) Run(stage *Stage) error {
	panic("Run should never be called on a selector Runner")
}

// OnAdd implements OnAdd interface for Selector
func (s *Selector) OnAdd(prevRunner Runner) {
	if asSelectable, isSelectable := prevRunner.(Selectable); isSelectable {
		asSelectable.SetSelection(s.selection...)
	} else {
		panic(fmt.Sprintf("Selector used after non-selectable runner %v", prevRunner))
	}
}

// NoOpRunner implements NoOpRunner interface for Selector
func (s *Selector) NoOpRunner() bool { return true }
