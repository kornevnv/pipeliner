package file

import (
	"bytes"
	"encoding/gob"
)

func encodeBinary[T any](data T) ([]byte, error) {
	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)
	if err := enc.Encode(data); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func decodeBinary[T any](buf []byte, data *T) (*T, error) {
	reader := bytes.NewReader(buf)
	dec := gob.NewDecoder(reader)
	if err := dec.Decode(data); err != nil {
		return nil, err
	}

	return data, nil
}
