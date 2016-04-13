package parse

import (
	"github.com/urbint/ingest"
)

// CSV returns an *ingest.Processor that will read a File
func CSV(mapper interface{}) *CSVProcessor {
	return &CSVProcessor{}
}

// A CSVProcessor is a processor that handles reading CSV files
type CSVProcessor struct {
	ingest.Processor
}
