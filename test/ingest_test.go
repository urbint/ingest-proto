package ingest_test

import (
	"github.com/urbint/ingest"
	// "github.com/urbint/ingest/parse"

	// . "github.com/smartystreets/goconvey/convey"
	"testing"
)

func TestIngest(t *testing.T) {
	// Convey("Ingest Acceptance", t, func() {
	// 	sink := &MockDestination{}

	// 	pipeline := ingest.Open("test/fixtures/").
	// 		Then(ingest.Select("file1.csv", "file2.csv")).
	// 		Then(parse.CSV(CSVMapper{})).
	// 		Then(sink).Build()

	// 	err := pipeline.Run()

	// 	So(err, ShouldBeNil)
	// 	So(sink.CallCount, ShouldEqual, 5)
	// })
}

type MockDestination struct {
	CallCount int
}

func (m *MockDestination) Run(stage *ingest.Stage) error {
	for range stage.In {
		m.CallCount++
	}
	return nil
}

type CSVMapper struct {
}
