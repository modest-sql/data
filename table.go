package main

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"log"
	"math/rand"
	"os"
	"time"
)

type table struct {
	field `json:"fields"`
}

type field struct {
	FLOAT32 float32 `json:"Float32"`
	FLOAT64 float64 `json:"Float64"`
	UINT32  uint32  `json:"Uint32"`
}

func _Create() {
	_WriteFile()
	_ReadFile()
}

func _Read() {
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
	r := rand.New(rand.NewSource(time.Now().UnixNano()))

	for i := 0; i < 10; i++ {

		s := &table{
			field{
				r.Float32(),
				r.Float64(),
				r.Uint32(),
			},
		}
		var binBuf bytes.Buffer
		binary.Write(&binBuf, binary.BigEndian, s)
		//b :=bin_buf.Bytes()
		//l := len(b)
		//fmt.Println(l)
		writeNextBytes(file, binBuf.Bytes())

	}
}

func writeNextBytes(file *os.File, bytes []byte) {

	_, err := file.Write(bytes)

	if err != nil {
		log.Fatal(err)
	}
}
