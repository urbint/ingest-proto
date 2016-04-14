package process

import (
	"archive/zip"
	"errors"
	"os"
	"regexp"

	"github.com/urbint/ingest"
)

// Unzipper is a Runner that will unzip a file
type Unzipper struct {
	filter []*regexp.Regexp
}

// Unzip receives an os.File and will Unzip it, emitting the files within
//
// It is selectable, allowing you to use a Regex to filter said files
func Unzip() *Unzipper {
	return &Unzipper{}
}

// Run implements ingest.Runner for Unzipper
func (u *Unzipper) Run(stage *ingest.Stage) error {
	for {
		select {
		case abortChan := <-stage.Abort:
			abortChan <- nil
			return nil
		case in, ok := <-stage.In:
			if !ok {
				return nil
			}
			file, isFile := in.(*os.File)
			if !isFile {
				return errors.New("Unzip received non *os.File input")
			}
			file.Close() // we don't need it open anymore, we are going to re-open it as a zip

			archive, err := zip.OpenReader(file.Name())
			if err != nil {
				return err
			}

			for _, innerFile := range archive.File {
				if u.filterMatch(innerFile.FileHeader.Name) {
					osFile, err := innerFile.Open()
					if err != nil {
						return err
					}
					stage.Send(osFile)
				}
			}
		}
	}
	return nil
}

// OnAdd implements ingest.OnAdd for Unzipper
//
// It will configure the ingest.Opener to have a tmp Directory (needed since zips need random access)
// If it is not already configured to do so.
func (u *Unzipper) OnAdd(prevRunner ingest.Runner) {
	if opener, isOpener := prevRunner.(*ingest.Opener); isOpener {
		if opener.Opts.TempDir == "" {
			opener.Opts.TempDir = "tmp/"
		}
	}
}

// SetSelection implements ingest.Selectable for Unzipper
//
// It will filter the contents of the extracted files for file names that match
// the regex provided by the selection
func (u *Unzipper) SetSelection(selections ...string) {
	for _, selection := range selections {
		u.filter = append(u.filter, regexp.MustCompile(selection))
	}
}

func (u *Unzipper) filterMatch(name string) bool {
	if len(u.filter) == 0 {
		return true
	}

	for _, regex := range u.filter {
		if regex.MatchString(name) {
			return true
		}
	}

	return false
}
