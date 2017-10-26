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
			create()
		case 2:
			getTable()
		case 3:
			read()
		case 4:
			CreateFile()
		case 5:
			deleteFile()
		case 6:
			os.Exit(2)
		default:
			printHelp()
		}
	}
}
