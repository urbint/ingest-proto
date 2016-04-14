package parse

import (
	"bytes"
	. "github.com/smartystreets/goconvey/convey"
	. "github.com/urbint/conveyer"
	"github.com/urbint/ingest"

	"testing"
)

func TestCSV(t *testing.T) {

	var SampleCSV = `user_id,age,favorite_color,name,city
1,42,Blue,Bob,Madison
2,17,Yellow,Steve O,New York
3,18,Blue,James,New York
4,22,Purple,Alice,Boston`

	type Person struct {
		ID     int    `csv:"user_id"`
		Name   string `csv:"name"`
		Age    int    `csv:"age"`
		HasPet bool
	}

	Convey("CSV", t, func() {
		parser := CSV(Person{})

		Convey("Parsing", func() {
			parser.ParseHeader([]string{"user_id", "age", "favorite_color", "name", "city"})
			Convey("Headers", func() {

				So(parser.fieldMap, ShouldResemble, map[int]int{
					0: 0,
					1: 2,
					2: -1,
					3: 1,
					4: -1,
				})
			})

			Convey("Rows", func() {
				res, err := parser.ParseRow([]string{"1", "38", "blue", "Steven", "New York"})
				So(err, ShouldBeNil)
				So(res, ShouldResemble, Person{
					ID:   1,
					Name: "Steven",
					Age:  38,
				})
			})
		})

		Convey("Run", func() {
			stage := ingest.NewStage()
			Convey("Works with io.Reader as In", func() {
				reader := bytes.NewBufferString(SampleCSV)
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
					ID: 3, Age: 17, Name: "James",
				})

			})
		})
	})
}
