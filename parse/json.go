package parse

import (
	"encoding/json"
	"github.com/urbint/ingest"
	"github.com/urbint/ingest/utils"
	"io"
	"reflect"
	"runtime"
	"strings"
	"sync"
)

type (
	// A JSONProcessor is used to process JSON via encoding/json
	JSONProcessor struct {
		mapper      interface{}
		newInstance func() reflect.Value
		logger      ingest.Logger

		workerOut      chan interface{}
		workerWg       sync.WaitGroup
		workerErr      chan error
		workersWorking chan bool
		workerQuit     chan bool

		opts    *JSONOpts
		sendPtr bool
	}

	// JSONOpts are options used to configre a JSONProcessor (and an FFJSONProcessor)
	JSONOpts struct {
		AbortOnFailedObject bool
		Selector            string
		NumDecoders         int
		Logger              ingest.Logger
	}
)

// JSON returns an *parse.JSONProcessor which will decode a JSON file to a specified struct
func JSON(mapper interface{}, opts ...JSONOpts) *JSONProcessor {
	opt := defaultJSONOpts()
	if len(opts) != 0 {
		utils.Extend(&opt, opts[0])
	}

	indirectType := reflect.Indirect(reflect.ValueOf(mapper)).Type()

	return &JSONProcessor{
		workersWorking: make(chan bool, opt.NumDecoders),
		workerErr:      make(chan error, opt.NumDecoders),
		workerOut:      make(chan interface{}, opt.NumDecoders),
		workerWg:       sync.WaitGroup{},
		newInstance:    func() reflect.Value { return reflect.New(indirectType) },
		mapper:         mapper,
		sendPtr:        reflect.TypeOf(mapper).Kind() == reflect.Ptr,
		logger:         opt.Logger,
		opts:           &opt,
	}
}

func defaultJSONOpts() JSONOpts {
	return JSONOpts{
		NumDecoders: runtime.NumCPU(),
		Logger:      ingest.DefaultLogger,
	}
}

// Name implements ingest.Runner for JSONProcessor
func (j *JSONProcessor) Name() string {
	return "JSON"
}

// Run implements ingest.Runner for JSONProcessor
func (j *JSONProcessor) Run(stage *ingest.Stage) error {
	j.workerQuit = make(chan bool)
	j.workerOut = make(chan interface{}, j.opts.NumDecoders)

	defer func() { close(j.workerQuit) }()

	in := stage.In

	for {
		select {
		case <-stage.Abort:
			return nil
		case data, more := <-j.workerOut:
			if !more {
				return nil
			}
			stage.Out <- data
		case err := <-j.workerErr:
			if j.opts.AbortOnFailedObject {
				return err
			}
			log := j.logger.WithError(err)
			if asUnmarshalTypeErr, isUnmarshalTypeErr := err.(*json.UnmarshalTypeError); isUnmarshalTypeErr {
				log = log.WithField("offset", asUnmarshalTypeErr.Offset).WithField("value", asUnmarshalTypeErr.Value)
			}
			log.Warn("Error unmarshalling JSON record")
		case input, ok := <-in:
			if !ok {
				// Set input to nil to not go in here any more, then wait for all the workers
				// to finish processing before closing output
				in = nil
				go func() {
					j.workerWg.Wait()
					close(j.workerOut)
				}()
				continue
			}
			rc, err := utils.ToIOReadCloser(input)
			if err != nil {
				return err
			}
			go j.handleIO(rc)
		}
	}
}

func (j *JSONProcessor) handleIO(rc io.ReadCloser) {
	j.workersWorking <- true
	j.workerWg.Add(1)
	go func() {
		defer func() {
			rc.Close()
			j.workerWg.Done()
			<-j.workersWorking
		}()

		decoder := json.NewDecoder(rc)
		if j.opts.Selector != "" {
			if err := j.navigateToSelection(decoder); err != nil {
				j.workerErr <- err
				return
			}
		}

		for {
			select {
			case <-j.workerQuit:
				return
			default:
				if !decoder.More() {
					return
				}
				rec := j.newInstance()
				if err := decoder.Decode(rec.Interface()); err != nil {
					if err == io.EOF {
						return
					}
					j.workerErr <- err
					continue
				}

				var toSend interface{}

				if j.sendPtr {
					toSend = rec.Interface()
				} else {
					toSend = rec.Elem().Interface()
				}

				select {
				case <-j.workerQuit:
					return
				case j.workerOut <- toSend:
				}
			}
		}
	}()
}

func (j *JSONProcessor) navigateToSelection(decoder *json.Decoder) error {
	nestIn := strings.Split(j.opts.Selector, ".")
	for len(nestIn) > 0 {
		token, err := decoder.Token()
		if err != nil {
			return err
		}
		if token == nestIn[0] {
			nestIn = nestIn[1:]
			continue
		} else if nestIn[0] == "*" {
			if delimVal, isDelim := token.(json.Delim); isDelim && json.Delim('[') == delimVal {
				nestIn = nestIn[1:]
			}
			continue
		}
	}
	return nil
}

// SkipAbortErr saves us having to send nil errors back on abort
func (j *JSONProcessor) SkipAbortErr() bool {
	return true
}

// SetSelection implements ingest.Selectable for JSONProcessor
func (j *JSONProcessor) SetSelection(selection ...string) {
	if len(selection) > 0 {
		j.opts.Selector = selection[0]
	}
}
