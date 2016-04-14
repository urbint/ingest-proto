package ingest

// Selectable is an interface that a Runner can implement
// which allows it to be filtered via the SetSelection method
type Selectable interface {
	Runner
	SetSelection(selection ...string)
}
