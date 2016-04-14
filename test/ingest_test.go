package ingest_test

import (
	"github.com/urbint/ingest"
	"github.com/urbint/ingest/parse"

	. "github.com/smartystreets/goconvey/convey"
	"testing"
)

func TestIngest(t *testing.T) {
	Convey("Ingest Acceptance", t, func() {
		out := make(chan interface{})

		pipeline := ingest.Open("../test/fixtures/").
			Then(ingest.Select("file1.csv", "file2.csv")).
			Then(parse.CSV(&Person{})).
			StreamTo(out).Build()

		err := pipeline.RunAsync()

		var results []interface{}
		go func() {
			for rec := range out {
				results = append(results, rec)
			}
		}()

		So(<-err, ShouldBeNil)
		So(results, ShouldHaveLength, 5)
	})
}

type Person struct {
	Name   string  `csv:"name"`
	Age    int     `csv:"age"`
	Weight float32 `csv:"weight"`
}
