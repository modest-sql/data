package main

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"log"
	"os"
)

type table struct {
	field `json:"fields"`
}

type field struct {
	Name int64 `json:"Name"`
}

func _Create() {
	_WriteFile()
}

func _Read() {
	_ReadFile()
}

func _Update() {

}

func _Delete() {

}

func _GetTable() {

}

func _ReadFile() {
	file, err := os.Open("data.bin")
	defer file.Close()

	if err != nil {
		log.Fatal(err)
	}

	m := table{}
	for i := 0; i < 10; i++ {
		data := readNextBytes(file, 16)
		buffer := bytes.NewBuffer(data)
		err = binary.Read(buffer, binary.BigEndian, &m)
		if err != nil {
			log.Fatal("binary.Read failed", err)
		}
		fmt.Println(m)
	}
}

func readNextBytes(file *os.File, number int) []byte {
	bytes := make([]byte, number)

	_, err := file.Read(bytes)
	if err != nil {
		log.Fatal(err)
	}

	return bytes
}

func _WriteFile() {
	file, err := os.Create("data.bin")
	defer file.Close()

	if err != nil {
		log.Fatal(err)
	}

	//Debug random tool
	for i := 0; i < 10; i++ {

		s := &table{
			field{
				Name: 'R',
			},
		}
		var binBuf bytes.Buffer
		binary.Write(&binBuf, binary.BigEndian, s)
		b := binBuf.Bytes()
		l := len(b)
		fmt.Println(l)
		writeNextBytes(file, binBuf.Bytes())

	}
}

func writeNextBytes(file *os.File, bytes []byte) {

	_, err := file.Write(bytes)

	if err != nil {
		log.Fatal(err)
	}
}
