package parse

import (
	"bytes"
	"encoding/csv"
	"errors"
	"fmt"
	"io"
	"reflect"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/urbint/ingest"
	"github.com/urbint/ingest/utils"
)

type (
	// A CSVProcessor is a processor that handles reading CSV files
	CSVProcessor struct {
		mapper interface{}

		logger ingest.Logger

		fieldMap map[int][]int

		opts    *CSVOpts
		sendPtr bool
	}

	//CSVOpts are options used to configure a CSVProcessor
	CSVOpts struct {
		// NumDecoders is the number of Go routines that will be used for processing
		// the CSV. Defaults to the number of CPU cores
		NumDecoders int

		// DateFormat is the format of Date strings used by the mapper to parse the dates
		DateFormat string

		// TrimSpaces determines whether spaces will be trimmed on fields
		TrimSpaces bool

		// FieldMap is a map of intergers representing the index of the column of the CSV row mapped
		// to the indicies of the field. On the struct(s if embedded).
		// If not specified, it will be generated using the first
		// row of the CSV mapped to struct tags of the mapper specified with "csv".
		FieldMap map[int][]int

		// SkipHeader determines whether a Header should be skipped
		SkipHeader bool

		// AbortOnFailedRow will cause the CSV parser to stop attempting to decode if it can't decode a row
		AbortOnFailedRow bool

		// Logger is the logger to be used. It defaults to the DefaultLogger set on ingest
		Logger ingest.Logger
	}
)

// HasCSVOpts is an interface that a mapper can implement to set CSV options by default
type HasCSVOpts interface {
	CSVOpts() CSVOpts
}

// CSV returns an *ingest.Processor that will read a File
func CSV(mapper interface{}, opts ...CSVOpts) *CSVProcessor {
	opt := defaultCSVOpts()

	if optionMaker, hasOpts := mapper.(HasCSVOpts); hasOpts {
		utils.Extend(&opt, optionMaker.CSVOpts())
	}

	if len(opts) != 0 {
		utils.Extend(&opt, opts[0])
	}

	processor := &CSVProcessor{
		mapper:   mapper,
		sendPtr:  reflect.TypeOf(mapper).Kind() == reflect.Ptr,
		fieldMap: opt.FieldMap,
		opts:     &opt,
	}
	processor.logger = opt.Logger.WithField("processor", processor.Name())

	return processor
}

// Default CSV  opts sets sane defaults for CSVOpts
func defaultCSVOpts() CSVOpts {
	return CSVOpts{
		NumDecoders: runtime.NumCPU(),
		DateFormat:  "01/02/2006",
		Logger:      ingest.DefaultLogger,
	}
}

// Name implements ingest.Runner for CSVProcessor
func (c *CSVProcessor) Name() string {
	return "CSV Reader"
}

// Run implements ingest.Runner for CSVProcessor
func (c *CSVProcessor) Run(stage *ingest.Stage) error {
	log := c.logger
	for {
		select {
		case <-stage.Abort:
			return nil
		case input, ok := <-stage.In:
			if !ok {
				return nil
			}
			if ioReader, isIOReader := input.(io.Reader); isIOReader {
				log.Debug("Got an io reader")
				if err := c.handleIOReader(stage, ioReader); err != nil {
					return err
				}
			} else if str, isString := input.(string); isString {
				log.Debug("Got a string")
				if err := c.handleIOReader(stage, bytes.NewBufferString(str)); err != nil {
					return err
				}
			} else if data, isBytes := input.([]byte); isBytes {
				log.Debug("Got a byte")
				if err := c.handleIOReader(stage, bytes.NewBuffer(data)); err != nil {
					return err
				}
			} else {
				return errors.New("Unknown input to CSV Reader")
			}
		}
	}
}

// ParseHeader builds a header map from a single row using
// the struct tags specified from the Map
func (c *CSVProcessor) ParseHeader(headers []string) {
	c.logger.Debug("Parsed header")

	targetType := reflect.Indirect(reflect.ValueOf(c.mapper)).Type()
	result := map[int][]int{}
	for column := 0; column < len(headers); column++ {
		header := strings.TrimSpace(headers[column])
		findResult, _ := findFieldInStruct(header, targetType)
		result[column] = findResult
	}

	c.fieldMap = result
}

// ParseRow parses a single row and returns a new instance of the
// same type as the mapper.
func (c *CSVProcessor) ParseRow(row []string) (interface{}, error) {
	instance := reflect.New(reflect.Indirect(reflect.ValueOf(c.mapper)).Type())

	fieldMap := c.fieldMap

	if fieldMap == nil {
		return nil, fmt.Errorf("No field map configured")
	}

	for j := 0; j < len(row); j++ {
		fieldIndicies := fieldMap[j]

		// If the length of the string is 0, or we don't have a mapping
		// keep the "nil" version of the struct field
		if len(fieldIndicies) == 0 || len(row[j]) == 0 {
			continue
		}

		field := instance.Elem()
		for _, fieldIndex := range fieldIndicies {
			field = field.Field(fieldIndex)
		}

		fieldInterface := field.Interface()
		switch fieldInterface.(type) {
		case string:
			if c.opts.TrimSpaces {
				field.SetString(strings.TrimSpace(row[j]))
			} else {
				field.SetString(row[j])
			}
		case float32:
			val, err := strconv.ParseFloat(row[j], 32)
			if err != nil {
				return nil, fmt.Errorf("Error parsing float: %v", row[j])
			}
			field.SetFloat(val)
		case int:
			val, err := strconv.Atoi(row[j])
			if err != nil {
				return nil, fmt.Errorf("Error parsing int: %v", row[j])
			}
			field.SetInt(int64(val))
		case int8:
			val, err := strconv.ParseInt(row[j], 10, 8)
			if err != nil {
				return nil, fmt.Errorf("Error parsing int: %v", row[j])
			}
			field.SetInt(val)
		case uint8:
			val, err := strconv.ParseUint(row[j], 10, 8)
			if err != nil {
				return nil, fmt.Errorf("Error parsing uint: %v", row[j])
			}
			field.SetUint(val)
		case uint16:
			val, err := strconv.ParseUint(row[j], 10, 16)
			if err != nil {
				return nil, fmt.Errorf("Error parsing uint: %v", row[j])
			}
			field.SetUint(val)
		case uint32:
			val, err := strconv.ParseUint(row[j], 10, 32)
			if err != nil {
				return nil, fmt.Errorf("Error parsing uint: %v", row[j])
			}
			field.SetUint(val)
		case time.Time:
			time, err := time.Parse(c.opts.DateFormat, row[j])
			if err != nil {
				return nil, fmt.Errorf("Error parsing date: %v", row[j])
			}
			field.Set(reflect.ValueOf(time))
		default:
			return nil, fmt.Errorf("Unhandled type: %v", field.Type().String())
		}
	}

	if c.sendPtr {
		return instance.Interface(), nil
	}

	return instance.Elem().Interface(), nil

}

// SkipAbortErr saves us having to send nil errors back on abort
func (c *CSVProcessor) SkipAbortErr() bool {
	return true
}

// handleIOReader handles an io.Reader input
func (c *CSVProcessor) handleIOReader(stage *ingest.Stage, input io.Reader) error {
	reader := csv.NewReader(input)

	if asCloser, isCloser := input.(io.Closer); isCloser {
		defer asCloser.Close()
	}

	if !c.opts.SkipHeader {
		header, err := reader.Read()
		if err != nil {
			return err
		}
		c.ParseHeader(header)
	}

	errors := make(chan error)
	defer func() { close(errors) }()

	rows := c.startCSVReader(reader, errors)
	parsed := c.startDecoders(rows, errors)

	for {
		select {
		case <-stage.Abort:
			return nil
		case err := <-errors:
			return err
		case rec, ok := <-parsed:
			if ok {
				select {
				case <-stage.Abort:
					return nil
				case stage.Out <- rec:
				}
			} else {
				return nil
			}
		}
	}
}

func (c *CSVProcessor) startCSVReader(reader *csv.Reader, errChan chan error) (output chan []string) {
	output = make(chan []string, c.opts.NumDecoders)

	go func() {
		defer func() { close(output) }()

		for {
			row, err := reader.Read()
			if err == io.EOF {
				return
			} else if parseErr, isParseError := err.(*csv.ParseError); isParseError && parseErr.Err == csv.ErrFieldCount {
				if !c.opts.AbortOnFailedRow {
					c.logger.WithError(err).Warn("Error parsing CSV row")
					continue
				}
				errChan <- err
				return
			} else if err != nil {
				c.logger.WithError(err).Error("Unknown error reading CSV")
				errChan <- err
				return
			}
			output <- row
		}
	}()

	return output
}

func (c *CSVProcessor) startDecoders(input chan []string, errChan chan error) (output chan interface{}) {
	workerCount := c.opts.NumDecoders
	output = make(chan interface{}, workerCount)

	go func() {
		// Start all of the workers and close done when they all exit
		wg := sync.WaitGroup{}
		wg.Add(workerCount)
		for i := 0; i < workerCount; i++ {
			go func() {
				defer wg.Done()
				for row := range input {
					rec, err := c.ParseRow(row)
					if err != nil && c.opts.AbortOnFailedRow {
						errChan <- err
						return
					} else if err != nil {
						continue
					}
					output <- rec
				}
			}()
		}
		wg.Wait()
		close(output)
	}()

	return output
}

func findFieldInStruct(fieldName string, target reflect.Type) (result []int, found bool) {
	numFields := target.NumField()
	for i := 0; i < numFields; i++ {
		field := target.Field(i)
		kind := field.Type.Kind()

		isEmbeddedStruct := kind == reflect.Struct || (kind == reflect.Ptr && field.Type.Elem().Kind() == reflect.Struct)

		if isEmbeddedStruct {
			var nestedTarget reflect.Type
			if kind == reflect.Struct {
				nestedTarget = field.Type
			} else {
				nestedTarget = field.Type.Elem()
			}
			nestedIndexes, found := findFieldInStruct(fieldName, nestedTarget)
			if found {
				result := append([]int{i}, nestedIndexes...)
				return result, true
			}
		} else {
			csvName := field.Tag.Get("csv")
			if csvName == fieldName {
				return []int{i}, true
			}
		}
	}
	return []int{}, false
}
