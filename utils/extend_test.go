package utils

import (
	. "github.com/smartystreets/goconvey/convey"
	// . "github.com/urbint/conveyer"

	"testing"
)

func TestExtend(t *testing.T) {
	type MyStruct struct {
		Num int
	}

	Convey("Extend", t, func() {
		Convey("Copies non-zero values", func() {
			result := &MyStruct{}
			Extend(result, MyStruct{Num: 2})

			So(result.Num, ShouldEqual, 2)
		})

		Convey("Does not copy non-zero values", func() {
			result := &MyStruct{Num: 4}
			Extend(result, MyStruct{})

			So(result.Num, ShouldEqual, 4)
		})
	})
}
