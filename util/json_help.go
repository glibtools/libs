package util

import (
	"bytes"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"io"

	"github.com/tidwall/gjson"
	"github.com/tidwall/sjson"
)

type Decoder = json.Decoder

type Encoder = json.Encoder

type RawMessage = json.RawMessage

func GJson(data []byte) gjson.Result { return gjson.ParseBytes(data) }

// JSONDump ...
func JSONDump(val interface{}, args ...interface{}) string {
	var indent bool
	if len(args) > 0 {
		indent, _ = args[0].(bool)
	}
	if indent {
		return string(JsMarshalIndent(val))
	}
	return string(JsMarshal(val))
}

func JSONEncodeDisableEscapeHTML(val interface{}) (bytesResult []byte, err error) {
	buf := bytes.NewBuffer(nil)
	enc := json.NewEncoder(buf)
	enc.SetEscapeHTML(false)
	if err = enc.Encode(val); err != nil {
		return
	}
	bytesResult = buf.Bytes()
	return
}

// JSONMarshalToBase64 ...
func JSONMarshalToBase64(val interface{}) ([]byte, error) {
	bts, err := json.Marshal(val)
	if err != nil {
		return bts, err
	}
	enc := base64.StdEncoding
	buf := make([]byte, enc.EncodedLen(len(bts)))
	enc.Encode(buf, bts)
	return buf, err
}

// JSONUnmarshalFromBase64 ...
func JSONUnmarshalFromBase64(data []byte, val interface{}) error {
	enc := base64.StdEncoding
	buf := make([]byte, enc.DecodedLen(len(data)))
	n, err := enc.Decode(buf, data)
	if err != nil {
		return err
	}
	bts := buf[:n]
	return json.Unmarshal(bts, val)
}

// JsMarshal ...
func JsMarshal(val interface{}) (bts []byte) { bts, _ = json.Marshal(val); return }

func JsMarshalHex(val interface{}) (dst []byte, err error) {
	b, err := json.Marshal(val)
	if err != nil {
		return
	}
	dst = make([]byte, hex.EncodedLen(len(b)))
	hex.Encode(dst, b)
	return
}

// JsMarshalIndent ...
func JsMarshalIndent(val interface{}) (bts []byte) {
	bts, _ = json.MarshalIndent(val, "", "\t")
	return
}

func JsUnmarshalHex(data []byte, val interface{}) (err error) {
	b := make([]byte, len(data))
	copy(b, data)
	n, err := hex.Decode(b, b)
	if err != nil {
		return
	}
	return json.Unmarshal(b[:n], val)
}

func Marshal(v interface{}) ([]byte, error) {
	return json.Marshal(v)
}

func MarshalIndent(v interface{}, prefix, indent string) ([]byte, error) {
	return json.MarshalIndent(v, prefix, indent)
}

// MarshalToString ...
func MarshalToString(v interface{}, args ...interface{}) (string, error) {
	_fn := func() ([]byte, error) {
		if len(args) > 0 && args[0] == true {
			return MarshalIndent(v, "", "  ")
		}
		return Marshal(v)
	}
	data, err := _fn()
	return string(data), err
}

func NewDecoder(r io.Reader) *Decoder {
	return json.NewDecoder(r)
}

func NewEncoder(w io.Writer) *Encoder {
	return json.NewEncoder(w)
}

func SetJson(data []byte, path string, val interface{}) []byte {
	bts, _ := sjson.SetBytes(data, path, val)
	return bts
}

func TrimByteSliceSpace(data []byte) []byte {
	var buf bytes.Buffer
	for _, b := range data {
		if b == ' ' || b == '\t' || b == '\n' || b == '\v' || b == '\f' || b == '\r' {
			continue
		}
		buf.WriteByte(b)
	}
	return buf.Bytes()
}

func Unmarshal(data []byte, v interface{}) error {
	return json.Unmarshal(data, v)
}

// UnmarshalFromString ...
func UnmarshalFromString(data string, v interface{}) error {
	return json.Unmarshal([]byte(data), v)
}
