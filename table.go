package main

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"log"
	"os"
)

type Table struct {
	TableName [16]byte `json:"table_name"`
	Field `json:"field"`
}

type Field struct {
	Name [8]byte `json:"name"`
}

func create() {
	t := Table{Field: Field{}}

	copy(t.TableName[:], "Empleados")
	copy(t.Name[:], "Roberto")
	WriteFile(t)

	copy(t.TableName[:], "Clientes")
	copy(t.Name[:], "Franks")
	WriteFile(t)

	copy(t.TableName[:], "Proveedores")
	copy(t.Name[:], "Andres")
	WriteFile(t)
}

func read() {
	readFile()
}

func update() {

}

func delete() {

}

func deleteAll() {
	
}

func getTable() {

}

func readFile() {
	file,_ := os.Open("data.bin")
	defer file.Close()

	fi, err := file.Stat()
	fmt.Printf("\nFile Size: %d\n\n", fi.Size())

	if err != nil {
		log.Fatal(err)
	}

	m := Table{}

	for i :=0 ; i < int(fi.Size()) ; i++ {
		data := readNextBytes(file, 24) //Tablas todavia manejadas por el tamano fijo
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

	b, _ := file.Read(bytes)
	fmt.Printf(string(b))

	return bytes
}

func WriteFile(class Table) {

	file, err := os.OpenFile("data.bin", os.O_APPEND|os.O_WRONLY, os.ModeAppend)
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

		var _, err = os.Stat("data.bin")

		if os.IsNotExist(err) {
			var file, err = os.Create("data.bin")
			if isError(err) { 
				return 
			}
			defer file.Close()
		}
	
		fmt.Println("\n==> Done creating file", "data.bin")
}

func deleteFile() {

	var err = os.Remove("data.bin")
	if isError(err) { return }

	fmt.Println("\n==> Done deleting file")
}

func isError(err error) bool {
	if err != nil {
		fmt.Println(err.Error())
	}

	return (err != nil)
}
