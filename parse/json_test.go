package parse

import (
	"bytes"
	. "github.com/smartystreets/goconvey/convey"
	. "github.com/urbint/conveyer"
	"github.com/urbint/ingest"
	"io/ioutil"
	"testing"
)

func TestJSON(t *testing.T) {
	var SampleJSON = `{"id":1,"age":42,"favoriteColor": "Blue","name": "Bob","city": "Madison" }
	{"id":2,"age":17,"favoriteColor": "Yellow","name": "Steve O","city": "New York" }
	{"id":3,"age":18,"favoriteColor": "Blue","name": "James","city": "New York" }
	{"id":4,"age":22,"favoriteColor": "Purple","name": "Alice","city": "Boston" }`

	type Base struct {
		Age int `json:"age"`
	}

	type Person struct {
		Base
		ID     int    `json:"id"`
		Name   string `json:"name"`
		HasPet bool
	}

	Convey("JSON", t, func() {
		stage := ingest.NewStage()
		parser := JSON(Person{})
		FocusConvey("works with encoding/json", func() {
			reader := bytes.NewBufferString(SampleJSON)
			go func() {
				stage.In <- reader
				close(stage.In)
			}()

			var err error
			go func() {
				err = parser.Run(stage)
				close(stage.Out)
			}()

			results := []Person{}

			for res := range stage.Out {
				results = append(results, res.(Person))
			}

			So(err, ShouldBeNil)
			So(results, ShouldHaveLength, 4)
			So(results, ShouldContainSomethingLike, Person{
				ID: 3, Base: Base{Age: 18}, Name: "James",
			})
		})

		Convey("navigating to a selection", func() {
			sampleJSON := `{"id":1,"nested":{"deeply":[1, 2, 3, 4]}}`

			Convey("works when selecting an entire array", func() {
				parser := JSON([]int{}, JSONOpts{Selector: "nested.deeply"})

				rc := ioutil.NopCloser(bytes.NewBufferString(sampleJSON))
				go parser.handleIO(rc)

				select {
				case err := <-parser.workerErr:
					So(err, ShouldBeNil)
				case rec := <-parser.workerOut:
					So(rec, ShouldResemble, []int{1, 2, 3, 4})
				}
			})

			Convey("works when iterating an array", func() {
				var result int

				parser := JSON(result, JSONOpts{Selector: "nested.deeply.*"})
				rc := ioutil.NopCloser(bytes.NewBufferString(sampleJSON))
				go parser.handleIO(rc)

				select {
				case err := <-parser.workerErr:
					So(err, ShouldBeNil)
				case rec := <-parser.workerOut:
					So(rec, ShouldEqual, 1)
				}
			})

		})
	})
}
