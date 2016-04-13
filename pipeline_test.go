package ingest

import (
	. "github.com/smartystreets/goconvey/convey"
	"testing"
)

func TestPipeline(t *testing.T) {
	Convey("Pipeline", t, func() {
		p := NewPipeline()
		firstProcessor := NewMockProcessor()
		secondProcessor := NewMockProcessor()

		res := p.Then(firstProcessor).Then(secondProcessor)

		Convey("Then", func() {
			Convey("Records the stage", func() {
				So(p.configs, ShouldHaveLength, 2)
			})

			Convey("Returns itself", func() {
				So(res, ShouldEqual, p)
			})
		})

		Convey("Build", func() {
			job := p.Build()
			Convey("Wires up channels between stages", func() {
			})
			Convey("Returns a Job", func() {
				So(job, ShouldNotBeNil)
			})
		})
	})
}
