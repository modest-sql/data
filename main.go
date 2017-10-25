package main

import (
	"fmt"
	"os"
)

func main() {

	printHelp()

	for {
		var input int
		n, err := fmt.Scanln(&input)
		if n < 1 || err != nil {
			fmt.Println("invalid input")
			return
		}

		switch input {
		case 1:
			_Create()
		case 2:
			_Read()
		case 3:
			_GetTable()
		case 4:
			printHelp()
		case 5:
			os.Exit(2)
		default:
			printHelp()
		}
	}
}
