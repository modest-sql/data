package main

import "fmt"

const tableNameLength = 16

func printHelp() {
	fmt.Printf("\n1.add\tAdd a table with the given name (maximum length %d)\n", tableNameLength)
	fmt.Printf("\n2.search\tsearch a table with the given name (maximum length %d)\n", tableNameLength)
	fmt.Printf("\n3.list\tList all available tables\n")
	fmt.Printf("\n4.help\tPrint this help page\n")
	fmt.Printf("\n5.exit\tThis does something iunno what\n")
	fmt.Printf("\n>> ")
}
