package ingest

import (
	. "github.com/smartystreets/goconvey/convey"
	"os"
	"testing"
)

func TestOpener(t *testing.T) {
	Convey("Opener", t, func() {
		Convey("With a directory", func() {
			opener := NewOpener("test/fixtures/")
			out := make(chan interface{})

			Convey("emits the directory contents", func() {
				err := NewPipeline().Then(opener).StreamTo(out).Build().RunAsync()
				results := []interface{}{}
				for file := range out {
					asFile := file.(*os.File)
					asFile.Close()
					results = append(results, file)
				}

				So(results, ShouldHaveLength, 2)
				So(<-err, ShouldBeNil)
			})

			Convey("can be filtered via select", func() {
				opener.SetSelection("file2.txt")

				err := NewPipeline().Then(opener).StreamTo(out).Build().RunAsync()
				results := []interface{}{}
				for file := range out {
					asFile := file.(*os.File)
					asFile.Close()
					results = append(results, file)
				}
				So(results, ShouldHaveLength, 1)
				So(<-err, ShouldBeNil)
			})
		})
	})
}
