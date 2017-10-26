package main

import (
	"fmt"
	"os"
)

func main() {

	printHelp()

	for {
		var input int
		fmt.Printf("\n>> ")
		n, err := fmt.Scanln(&input)
		fmt.Println()
		
		if n < 1 || err != nil {
			fmt.Println("invalid input")
			return
		}

		switch input {
		case 1:
			_Create()
		case 2:
			_GetTable()
		case 3:
			_Read()
		case 4:
			_CreateFile()
		case 5:
			_DeleteFile()
		case 6:
			os.Exit(2)
		default:
			printHelp()
		}
	}
}
