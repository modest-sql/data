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
	One float32 `json:"one"`
	Two float64 `json:"two"`
	Three uint32 `json:"three"`
}

func _Create() {
	_WriteFile(table{field{One: 1.11}})
	_WriteFile(table{field{Two: 2.22}})
	_WriteFile(table{field{Three: 3}})
	_WriteFile(table{field{One: 1.11}})
	_WriteFile(table{field{Two: 2.22}})
	_WriteFile(table{field{Three: 3}})
	_WriteFile(table{field{One: 1.11}})
	_WriteFile(table{field{Two: 2.22, Three: 3}})
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

	fi, err := file.Stat()
	fmt.Println(fi.Size())
	if err != nil {
		log.Fatal(err)
	}

	m := table{}
	for i :=0 ; i< 10 ; i++ {
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

func _WriteFile(class table) {
	file, err := os.OpenFile("data.bin", os.O_APPEND|os.O_WRONLY, os.ModeAppend) //Everytime calling this funciton file will reset keep that in mind
	defer file.Close()

	if err != nil {
		log.Fatal(err)
	}

		var binBuf bytes.Buffer
		binary.Write(&binBuf, binary.BigEndian, class)

		b := binBuf.Bytes()
		l := len(b)
		fmt.Println(l)

		writeNextBytes(file, binBuf.Bytes())
}

func writeNextBytes(file *os.File, bytes []byte) {

	_, err := file.Write(bytes)

	if err != nil {
		log.Fatal(err)
	}
}

func CreateFile() {
	
}