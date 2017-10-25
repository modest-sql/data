package main

import (
	"fmt"
	"os"
)

func main() {

	printHelp()

	for {
		var input string
		inpt, _ := fmt.Scanln(&input)

		switch inpt {
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
