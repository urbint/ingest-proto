package ingest

import (
	"io"
	"os"
	"path/filepath"
	"regexp"

	"github.com/alexflint/go-cloudfile"
	"github.com/urbint/ingest/utils"
)

// writeFileBlockSize is the number of bytes copied to the temporary file buffer
// each time before checking whether to abort again
const writeFileBlockSize = 1024

// An Opener is a Runner that opens files
type Opener struct {
	Opts   OpenOpts
	logger Logger
	path   string
	filter []*regexp.Regexp
}

// OpenOpts is used to configure how a file is opened
type OpenOpts struct {
	// If TempDir is specified, remote files will be downloaded to a temporary directory in full before
	// being being emitted to the next stage
	TempDir string

	// Logger is the logger that the Opener will log to
	Logger Logger

	// StreamProgressTo is used to specify a channel which will receive a count of bytes for files that are being downloaded
	// Currently only works if TempDir is specified. Opening will NOT be blocked by this channel blocking
	StreamProgressTo chan int64
}

// defaultOpenOpts sets sane defaults for OpenOpts
func defaultOpenOpts() OpenOpts {
	return OpenOpts{
		Logger: DefaultLogger,
	}
}

// NewOpener builds an Opener which will open the specified path
func NewOpener(path string, opts ...OpenOpts) *Opener {
	var opt = defaultOpenOpts()
	if len(opts) != 0 {
		utils.Extend(&opt, opts[0])
	}

	return &Opener{
		logger: opt.Logger,
		path:   path,
		Opts:   opt,
	}
}

// Open is a shortcut to create a new Pipeline that starts with the
// specified path or file.
//
// If the path is a directory, files can be selected from the directory using ingest.Select
// If the path is a file, the file will be emitted to the next Processor
func Open(path string, opts ...OpenOpts) *Pipeline {
	return NewPipeline().Then(NewOpener(path, opts...))
}

// Name implements the Runner interface for the Opener
func (o *Opener) Name() string {
	return "Opener"
}

// Run implements Runner for Opener
func (o *Opener) Run(stage *Stage) error {
	if stage.Out == nil {
		return nil // Nothing to do here
	}

	log := o.logger.WithField("file", o.path)

	log.Info("Opening file")
	file, err := cloudfile.Open(o.path)
	if err != nil {
		return err
	}

	if osFile, isOSFile := file.(*os.File); isOSFile {
		stat, err := osFile.Stat()
		if err != nil {
			return err
		}
		if stat.IsDir() {
			return o.emitDirectoryTo(osFile, stage.Out)
		}
		stage.Out <- osFile
	} else {
		if o.Opts.TempDir == "" {
			stage.Out <- file
		} else {
			file, err := o.writeBufferToTemp(file, stage.Abort)
			if file != nil {
				log.Info("Finished downloading file")
				stage.Out <- file
			}
			if err != nil {
				return err
			}
		}
	}

	return nil
}

// writeBufferToTemp will take a io.Reader and write it to a temp file based off of the opener.path
//
// If abort sends a result, it will stop
func (o *Opener) writeBufferToTemp(reader io.Reader, abort <-chan chan error) (*os.File, error) {
	_, outName := filepath.Split(o.path)
	err := os.MkdirAll(o.Opts.TempDir, 0770)
	if err != nil {
		return nil, err
	}

	if asCloser, canClose := reader.(io.Closer); canClose {
		defer asCloser.Close()
	}

	outFile, err := os.Create(filepath.Join(o.Opts.TempDir, outName))
	if err != nil {
		return nil, err
	}

	for {
		select {
		case <-abort:
			outFile.Close()
			return nil, nil
		default:
			if byteCount, err := io.CopyN(outFile, reader, writeFileBlockSize); err != nil {
				if o.Opts.StreamProgressTo != nil {
					go func() { o.Opts.StreamProgressTo <- byteCount }()
				}
				if err == io.EOF {
					return outFile, nil
				}
				outFile.Close()
				return nil, err
			}
		}
	}
}

// SetSelection implements ingest.Selectable for Opener
func (o *Opener) SetSelection(selection ...string) {
	for _, str := range selection {
		o.filter = append(o.filter, regexp.MustCompile(str))
	}
}

// OnPipelineDone implements ingest.OnDone for Opener
func (o *Opener) OnPipelineDone() error {
	if o.Opts.TempDir != "" {
		if err := os.RemoveAll(o.Opts.TempDir); err != nil {
			return err
		}
	}

	return nil
}

// SkipAbortErr saves us having to send nil errors back on abort
func (o *Opener) SkipAbortErr() bool {
	return true
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
