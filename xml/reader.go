package xml

import (
	"encoding/xml"
	"errors"
	"golang.org/x/net/html/charset"
	"io"
	"os"
	"strings"
)

// converts a file to records ((data, error) tuples)
type Reader struct {
	xmlFile *os.File
	decoder *xml.Decoder
}

type Record struct {
	TypeName string
	Data     interface{}
}

type RecordsBuilderResult struct {
	Records []*Record
	Err     error
}

type ProcessTokenResult struct {
	Records       []*Record
	IsEndOfStream bool
	Err           error
}

type RecordsBuilderFunction func(xml.Token) RecordsBuilderResult

func RecordArrayFromInterfaceArray(batch []interface{}, isEndOfStream bool) ProcessTokenResult {
	var records = make([]*Record, len(batch))

	// loop through the whole batch, casting each to the
	// correct type from interface{}
	for i, item := range batch {
		records[i] = item.(*Record)
	}

	return ProcessTokenResult{
		Records:       records,
		IsEndOfStream: isEndOfStream,
	}
}

func (r *Reader) Open(filename string) error {
	var err error
	if r.xmlFile, err = os.Open(filename); err != nil {
		return err
	}

	// read the first 64 characters to detect the encoding
	first64 := make([]byte, 64)
	if _, err = r.xmlFile.Read(first64); err != nil {
		return err
	}
	_, name, _ := charset.DetermineEncoding(first64, "text/xml")
	if _, err := r.xmlFile.Seek(0, 0); err != nil {
		return err
	}

	// if no encoding could be determined, attempt to fall back to UTF-8
	if name == "" {
		name = "UTF-8"
	}

	// create a reader label for the encoding we assume the file is, falling back to a UTF-8 encoding
	nr, _ := charset.NewReaderLabel(name, r.xmlFile)
	if nr == nil {
		nr, _ = charset.NewReaderLabel("UTF-8", r.xmlFile)
	}

	// if no reader label could be created, error out
	if nr == nil {
		return errors.New("could not allocate reader label")
	}

	r.decoder = xml.NewDecoder(nr)
	r.decoder.CharsetReader = func(set string, input io.Reader) (reader io.Reader, e error) {

		// html charset map in Go does not include UCS-2
		if strings.ToLower(set) == "ucs-2" {
			set = "utf-16le"
		}
		return charset.NewReaderLabel(set, input)
	}

	return nil
}

func (r *Reader) Close() error {
	if r.xmlFile != nil {
		return r.xmlFile.Close()
	}
	return nil
}

func (r *Reader) BuildRecordsFromToken(recordsBuilder RecordsBuilderFunction) ProcessTokenResult {

	// decode a token
	t, err := r.decoder.Token()

	// return an error, if one happened
	if err != nil {
		if err == io.EOF {
			return ProcessTokenResult{nil, true, nil}
		}

		return ProcessTokenResult{nil, false, err}
	}

	// stop looping when we have no more tokens
	if t == nil {
		return ProcessTokenResult{nil, true, nil}
	}

	res := recordsBuilder(t)
	return ProcessTokenResult{res.Records, false, res.Err}
}

func (r *Reader) DecodeToken(v interface{}, start *xml.StartElement) error {
	return r.decoder.DecodeElement(v, start)
}
