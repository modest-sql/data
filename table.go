package main

import (
	"fmt"
	"log"
	"encoding/json"
	"reflect"
)

type Table struct {
	TableName [50]byte `json:"Table_Name"`
	Field
}

type Field struct {
	Id	int32	`json:"Id"`
	Name [50]byte `json:"Name"`
}

func Create(tableName [50]byte) {

	var jsonText string
	
	var idents []Table

	if err := json.Unmarshal([]byte(jsonText), &idents); err != nil {
		log.Println(err)
	}

	table := Table {
		TableName: tableName,
		Field: Field{
		},
	}

	idents = append(idents, table)

	result, err := json.Marshal(idents)

	if err != nil {
		log.Println(err)
	}

	fmt.Println(string(result))

	WriteFile(result)
}

func Insert(tableName [50]byte) {
	
	var jsonText = ReadFile()

	var idents []Table

	if err := json.Unmarshal([]byte(jsonText), &idents); err != nil {
		log.Println(err)
	}

	var newJsonObj []Table

	for _, jsonObj := range idents{
		if reflect.DeepEqual(tableName, jsonObj.TableName){
			jsonObj.Id = getNextId(tableName)
			copy(jsonObj.Name[:], "Roberto")
		}
		
		newJsonObj = append(newJsonObj, jsonObj)
	
		result, err := json.Marshal(newJsonObj)
	
		if err != nil {
			log.Println(err)
		}
	
		fmt.Println(string(result))
	
		WriteFile(result)
	}
	return
}

func Update(tableName [50]byte, id int32) {
		
	var jsonText = ReadFile()
	var idents []Table
	
	if err := json.Unmarshal([]byte(jsonText), &idents); err != nil {
		log.Println(err)
	}
	
	var newJsonObj []Table

	for _, jsonObj := range idents{
		if jsonObj.Id == int32(id){
			copy(jsonObj.Name[:], "Andres")
		}
		newJsonObj = append(newJsonObj, jsonObj)
	}

	result, _ := json.Marshal(newJsonObj)
	fmt.Println(string(result))
	WriteFile(result)
}

func getNextId(tableName [50]byte) int32 {
	
	var jsonText = ReadFile()
	var idents []Table
	
	if err := json.Unmarshal([]byte(jsonText), &idents); err != nil {
		log.Println(err)
	}
		
	current_id := 0
	last_id := 0

	for _, jsonObj := range idents{
		if int(jsonObj.Id) > current_id {
			current_id = int(jsonObj.Id)
			last_id = current_id
		} 
	}
	fmt.Printf("%d",last_id)

	return int32(current_id + 1)
}

func ShowRegisters(tableName [50]byte) {
	var jsonText = ReadFile()
	
	var idents []Table
	
	if err := json.Unmarshal([]byte(jsonText), &idents); err != nil {
		log.Println(err)
	}

	fmt.Printf("Id |         Nombre         |\n");	
	fmt.Printf("---|------------------------|\n");

	for _, jsonObj := range idents {
		if	reflect.DeepEqual(tableName, jsonObj.TableName){
			fmt.Printf("%d  |%s                 |\n", jsonObj.Id, string(jsonObj.Name[:50]));
		}
	}
	fmt.Println()
}
 

func main() {
	
	for {
		
		fmt.Printf("\n~~~~~~~~~~~~~~~~~~~~~~~~~~~~Options~~~~~~~~~~~~~~~~~~~~~~~~~~~\n")
		fmt.Printf("\n1.insert\tInsert Attributes\n")
		fmt.Printf("\n2.update\t(Not Implemented correctly)\n")
		fmt.Printf("\n3.delete\t(Not Implemented)\n")
		fmt.Printf("\n4.show registers\tShow all registers in file\n")
		fmt.Printf("\n5.create table\tcreates table\n")

		var input int
		fmt.Printf("\n>> ")
		fmt.Scanln(&input)
		fmt.Println()

		switch input {
		case 1:
			var tableName [50]byte
			copy(tableName[:], "Empleado")

			Insert(tableName)
			break
		case 2:
			var id int32
			var tableName string
			fmt.Printf("\nId: ")
			fmt.Scanln(&id)
			fmt.Printf("\nTable Name: ")
			fmt.Scanln(&tableName)
			fmt.Println()

			//Update(tableName,id)
			break
		case 3:

		case 4:
			var tableName [50]byte
			copy(tableName[:], "Empleado")

			ShowRegisters(tableName);	
			break
		case 5:
			var tableName [50]byte
			copy(tableName[:], "Empleado")

			Create(tableName)
			break
		default:
			break
		}
	}
}