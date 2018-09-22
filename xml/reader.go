package xml

import (
	"encoding/xml"
	"io"
	"os"
)

type Reader struct {
	xmlFile *os.File
	decoder *xml.Decoder
}

func (r *Reader) Open(filename string) error {
	var err error
	r.xmlFile, err = os.Open(filename)

	if err != nil {
		return err
	}

	r.decoder = xml.NewDecoder(r.xmlFile)

	return nil
}

func (r *Reader) ProcessToken(process func(xml.Token) error) (isEOF bool, err error) {

	// decode a token
	t, err := r.decoder.Token()

	// return an error, if one happened
	if err != nil {
		if err == io.EOF {
			return true, nil
		}

		return false, err
	}

	// stop looping when we have no more tokens
	if t == nil {
		return true, nil
	}

	return false, process(t)
}

func (r *Reader) DecodeToken(v interface{}, start *xml.StartElement) {
	r.decoder.DecodeElement(v, start)
}
