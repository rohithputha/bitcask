package data

import (
	"encoding/binary"
	"encoding/json"
	"log"
)

type Ende struct {
}

func NewEnde() *Ende {
	return &Ende{}
}

func (e *Ende) EncodeData(timestamp int64, key interface{}, value interface{}) []byte {
	keyBytes, err := json.Marshal(key)
	valueBytes, err := json.Marshal(value)

	s := byte('\r')

	keyLengthBytes := make([]byte, 4)
	binary.BigEndian.PutUint32(keyLengthBytes, uint32(len(keyBytes)))

	valueLengthBytes := make([]byte, 4)
	binary.BigEndian.PutUint32(valueLengthBytes, uint32(len(valueBytes)))

	timestampBytes := make([]byte, 8)
	binary.BigEndian.PutUint64(timestampBytes, uint64(timestamp))

	if err != nil {
		return nil
	}

	finalBytes := append(keyLengthBytes, valueLengthBytes...)
	finalBytes = append(finalBytes, timestampBytes...)
	finalBytes = append(finalBytes, keyBytes...)
	finalBytes = append(finalBytes, valueBytes...)
	finalBytes = append(finalBytes, s)
	return finalBytes
}

func (e *Ende) DecodeData(data []byte) (int64, interface{}, interface{}) {
	if data == nil || len(data) <= 0 {
		return -1, nil, nil
	}
	if data[len(data)-1] != byte('\r') {
		log.Fatalf("Does not end with required end character")
		return -1, nil, nil
	}

	keyLength := binary.BigEndian.Uint32(data[0:4])
	valueLength := binary.BigEndian.Uint32(data[4:8])
	timestamp := binary.BigEndian.Uint64(data[8:16])
	key := data[16 : 16+keyLength]
	value := data[16+keyLength : 16+keyLength+valueLength]

	var keyInterface interface{}
	var valueInterface interface{}

	err := json.Unmarshal(key, &keyInterface)
	if err != nil {
		return -1, nil, nil
	}

	err = json.Unmarshal(value, &valueInterface)
	if err != nil {
		return -1, nil, nil
	}

	return int64(timestamp), keyInterface, valueInterface
}
