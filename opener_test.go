package ingest

import (
	"github.com/jarcoal/httpmock"
	. "github.com/smartystreets/goconvey/convey"
	"io"
	"io/ioutil"
	"os"
	"testing"
)

func TestOpener(t *testing.T) {
	Convey("Opener", t, func() {
		Convey("With a directory", func() {
			opener := NewOpener("test/fixtures/dirtest")
			out := make(chan interface{})

			Convey("emits the directory contents", func() {
				err := NewPipeline().Then(opener).StreamTo(out).Build().RunAsync()
				results := []interface{}{}
				for file := range out {
					asFile := file.(*os.File)
					asFile.Close()
					results = append(results, file)
				}

				So(results, ShouldHaveLength, 3)
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

		Convey("with HTTP", func() {
			httpmock.Activate()
			httpmock.RegisterResponder("GET", "http://google.com", httpmock.NewStringResponder(200, `Hello world`))

			Reset(func() {
				httpmock.DeactivateAndReset()
			})

			out := make(chan interface{})

			Convey("emits an io.Reader", func() {
				opener := NewOpener("http://google.com")
				errChan := NewPipeline().Then(opener).StreamTo(out).Build().RunAsync()
				result := (<-out).(io.Reader)
				buf, err := ioutil.ReadAll(result)
				So(err, ShouldBeNil)
				So(string(buf), ShouldEqual, "Hello world")

				So(<-errChan, ShouldBeNil)
			})

			Convey("with TempDir specified", func() {
				opener := NewOpener("http://google.com", OpenOpts{TempDir: "test/tmp"})
				errChan := NewPipeline().Then(opener).StreamTo(out).Build().RunAsync()

				Convey("emits an os.File", func() {
					rec := <-out

					So(<-errChan, ShouldBeNil)
					So(rec, ShouldHaveSameTypeAs, &os.File{})
				})

				Convey("with StreamProgressTo set", func() {
					out := make(chan interface{})
					progress := make(chan int64)

					opener := NewOpener("http://google.com", OpenOpts{TempDir: "test/tmp", StreamProgressTo: progress})
					errChan := NewPipeline().Then(opener).StreamTo(out).Build().RunAsync()

					<-out
					So(<-errChan, ShouldBeNil)

					bytes := <-progress
					So(bytes, ShouldBeGreaterThan, 0)
				})

				Convey("removes TempDir on OnPipelineDone", func() {
					err := opener.OnPipelineDone()
					So(err, ShouldBeNil)

					_, err = os.Stat("test/tmp")
					So(os.IsNotExist(err), ShouldBeTrue)
				})
			})
		})
	})
}
