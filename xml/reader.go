package xml

import (
	"encoding/xml"
	"golang.org/x/net/html/charset"
	"io"
	"os"
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

	r.decoder = xml.NewDecoder(r.xmlFile)
	r.decoder.CharsetReader = charset.NewReaderLabel

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
