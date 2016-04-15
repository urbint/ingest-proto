package ingest

import (
	"errors"
	"time"

	. "github.com/smartystreets/goconvey/convey"
	. "github.com/urbint/conveyer"
	"testing"
)

func TestJob(t *testing.T) {
	Convey("Job", t, func() {

		Convey("Start", func() {
			processor := NewMockProcessor()
			NewPipeline().Then(processor).Build().Start()

			Convey("Starts the processors", func() {
				time.Sleep(5 * time.Millisecond)
				So(processor.Started, ShouldBeTrue)
				So(processor.RunCalledWith, ShouldLookLike, Stage{})
			})
		})

		Convey("Wait", func() {
			Convey("Waits for the job to finish", func() {
				processor := NewMockProcessor(MockOpt{Wait: 10 * time.Millisecond})
				job := NewPipeline().Then(processor).Build()
				start := time.Now()

				job.Start().Wait()
				finish := time.Now()

				So(finish, ShouldHappenAfter, start.Add(2*time.Millisecond))
			})
			Convey("Returns errors from processing", func() {
				processor := NewMockProcessor(MockOpt{Err: errors.New("Mock Error")})
				err := NewPipeline().Then(processor).Build().Start().Wait()

				So(err, ShouldNotBeNil)
				So(err, ShouldHaveMessage, "Mock Error")
			})
		})

		Convey("Abort", func() {

			Convey("Emits error channels to all running workers", func() {
				processor := NewMockProcessor(MockOpt{Wait: time.Minute})
				job := NewPipeline().Then(processor).Build().Start()
				job.Abort()
				time.Sleep(10 * time.Millisecond)
				So(processor.Aborted, ShouldBeTrue)
			})
			Convey("Returns a channel of errors encountered while aborting", func() {
				processor := NewMockProcessor(MockOpt{Wait: time.Minute, Err: errors.New("Mock error")})
				job := NewPipeline().Then(processor).Build().Start()
				errs := job.Abort()
				time.Sleep(10 * time.Millisecond)

				So(processor.Aborted, ShouldBeTrue)

				errsEmitted := 0
				for err := range errs {
					errsEmitted++
					So(err, ShouldHaveMessage, "Mock error")
				}
				So(errsEmitted, ShouldEqual, 1)
			})
		})
	})
}
