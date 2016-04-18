package utils

import (
	"bytes"
	"errors"
	"io"
	"io/ioutil"
)

// ToIOReadCloser converts the specified input to an IOReadCloser.
//
// If it cannot be converted, an error will be returned.
// If the interface is an IOReader and does not implement
// a Close method, a NoOp close method will be added
func ToIOReadCloser(input interface{}) (io.ReadCloser, error) {
	if rc, isRC := input.(io.ReadCloser); isRC {
		return rc, nil
	}

	reader, err := ToIOReader(input)
	if err != nil {
		return nil, errors.New("Unable to convert type to io.ReadCloser")
	}

	return ioutil.NopCloser(reader), nil
}

// ToIOReader converts the specified input to an IOReader
func ToIOReader(input interface{}) (io.Reader, error) {
	if reader, isReader := input.(io.Reader); isReader {
		return reader, nil
	} else if str, isString := input.(string); isString {
		return bytes.NewBufferString(str), nil
	} else if buf, isBytes := input.([]byte); isBytes {
		return bytes.NewBuffer(buf), nil
	}
	return nil, errors.New("Unable to convert type to io.Reader")
}
