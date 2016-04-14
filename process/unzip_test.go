package process

import (
	. "github.com/smartystreets/goconvey/convey"
	"github.com/urbint/ingest"

	"io"
	"testing"
)

func TestUnzip(t *testing.T) {
	Convey("Unzip", t, func() {
		out := make(chan interface{})
		results := []interface{}{}

		Convey("emits files in a directory", func() {
			err := ingest.Open("../test/fixtures/example.zip").Then(Unzip()).StreamTo(out).Build().RunAsync()
			for file := range out {
				asCloser := file.(io.ReadCloser)
				asCloser.Close()
				results = append(results, asCloser)
			}

			So(<-err, ShouldBeNil)
			So(results, ShouldHaveLength, 4)
		})

		Convey("is selectable", func() {
			err := ingest.Open("../test/fixtures/example.zip").
				Then(Unzip()).
				Then(ingest.Select("file_[2,3].txt")).
				StreamTo(out).Build().RunAsync()

			for file := range out {
				asCloser := file.(io.ReadCloser)
				asCloser.Close()
				results = append(results, asCloser)
			}

			So(<-err, ShouldBeNil)
			So(results, ShouldHaveLength, 2)
		})
	})
}
