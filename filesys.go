package data

import(
	"bytes"
	"encoding/binary"
	"fmt"
	"log"
	"os"
)

func WriteFile(data []byte) {

	file, err := os.OpenFile("Data.bin", os.O_RDWR, 0644)
	defer file.Close()

	if err != nil {
		log.Fatal(err)
	}

		var binBuf bytes.Buffer
		binary.Write(&binBuf, binary.BigEndian, data)

		b := binBuf.Bytes()
		l := len(b)	
		fmt.Println("\nFile Size: ",l)

		writeNextBytes(file, binBuf.Bytes())
}
	
func writeNextBytes(file *os.File, bytes []byte) {

	_, err := file.Write(bytes)

	if err != nil {
		log.Fatal(err)
	}
}

func ReadFile() []byte {

	file,_ := os.Open("Data.bin")
	defer file.Close()

	fi, _ := file.Stat()

	result := readNextBytes(file, int(fi.Size()))
	fmt.Printf("Format: %s\n\n", result)

	return result
}

func readNextBytes(file *os.File, number int) []byte {
	
	bytes := make([]byte, number)
	file.Read(bytes)
	return bytes
}
