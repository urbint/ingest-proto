package ingest

// DefaultLogger is where ingest will log to. By default, it wont log. We recommend
// setting it to Logrus, or Apex, or your own.
var DefaultLogger Logger = &EmptyLogger{}

// Logger is a logging interface which mirrors pico.Logger, apex.Logger, and logrus.Logger
type Logger interface {
	WithError(err error) Logger
	Debug(args ...interface{})
	Info(args ...interface{})
	Warn(args ...interface{})
	Error(args ...interface{})
}

// EmptyLogger is the Default Logger used. It wont log. We recommend you set it to something better.
type EmptyLogger struct{}

// WithError placeholder
func (e *EmptyLogger) WithError(err error) Logger { return e }

// Debug placeholder
func (e *EmptyLogger) Debug(args ...interface{}) {}

// Info placeholder
func (e *EmptyLogger) Info(args ...interface{}) {}

// Warn placeholder
func (e *EmptyLogger) Warn(args ...interface{}) {}

// Error placeholder
func (e *EmptyLogger) Error(args ...interface{}) {}
