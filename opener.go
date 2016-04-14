package ingest

import (
	"github.com/alexflint/go-cloudfile"
	"os"
	"path/filepath"
	"regexp"
)

// An Opener is a Runner that opens files
type Opener struct {
	path string

	filter []*regexp.Regexp
}

// NewOpener builds an Opener which will open the specified path
func NewOpener(path string) *Opener {
	return &Opener{
		path: path,
	}
}

// Run implements Runner for Opener
func (o *Opener) Run(stage *Stage) error {
	if stage.Out == nil {
		return nil // Nothing to do here
	}

	file, err := cloudfile.Open(o.path)
	if err != nil {
		return err
	}

	if osFile, isOSFile := file.(*os.File); isOSFile {
		return o.emitDirectoryTo(osFile, stage.Out)
	}

	return nil
}

// SetSelection implements ingest.Selectable for Opener
func (o *Opener) SetSelection(selection ...string) {
	for _, str := range selection {
		o.filter = append(o.filter, regexp.MustCompile(str))
	}
}

// emitDirectoryTo will recursively traverse a directory and emit all files (matching the filter, if any)
// to the specified channel.
func (o *Opener) emitDirectoryTo(dir *os.File, out chan interface{}) error {
	files, err := dir.Readdir(0)
	if err != nil {
		return err
	}

	for _, fileInfo := range files {
		fullPath := filepath.Join(dir.Name(), fileInfo.Name())
		file, err := os.Open(fullPath)
		if err != nil {
			return err
		}

		if fileInfo.IsDir() {
			if err := o.emitDirectoryTo(file, out); err != nil {
				return err
			}
		} else {
			if o.fileMatchesSelection(file) {
				out <- file
			}
		}
	}
	return nil
}

// fileMatchesSelection checks whether the files name matches any of the applied filters.
//
// If no filters are specified it will return true
func (o *Opener) fileMatchesSelection(file *os.File) bool {
	if len(o.filter) == 0 {
		return true
	}

	for _, f := range o.filter {
		if f.MatchString(file.Name()) {
			return true
		}
	}
	return false
}
