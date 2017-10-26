package main

import "fmt"

const tableNameLength = 16

func printHelp() {
	fmt.Printf("\n~~~~~~~~~~~~~~~~~~~~~~~~~~~~Options~~~~~~~~~~~~~~~~~~~~~~~~~~~\n")
	fmt.Printf("\n1.add\tCreate Hardcoded Tables\n")
	fmt.Printf("\n2.search\tsearch a table with the given data (ignore this, still working on it)\n")
	fmt.Printf("\n3.list\tList all available tables\n")
	fmt.Printf("\n4.create\tcreates db file\n")
	fmt.Printf("\n5.delete\tdeletes db file\n")
	fmt.Printf("\n6.exit\tThis does something iunno what\n")
}
