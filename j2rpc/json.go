package j2rpc

import (
	"bytes"
	"encoding/json"
	"io"
)

type Decoder = json.Decoder

type Delim = json.Delim

type Encoder = json.Encoder

type RawMessage = json.RawMessage

func JSONDecode(data []byte, v interface{}) error {
	decoder := NewDecoder(bytes.NewReader(data))
	return decoder.Decode(v)
}

func JSONEncode(v interface{}) ([]byte, error) {
	buf := bytes.NewBuffer(nil)
	encoder := NewEncoder(buf)
	encoder.SetEscapeHTML(false)
	if err := encoder.Encode(v); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func NewDecoder(r io.Reader) *Decoder { return json.NewDecoder(r) }

func NewEncoder(w io.Writer) *Encoder { return json.NewEncoder(w) }
