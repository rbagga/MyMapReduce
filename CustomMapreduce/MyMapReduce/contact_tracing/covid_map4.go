package main

import (
	"fmt"
	"io/ioutil"
	"os"
	"strings"
)

func main() {
	input_file := os.Args[1]

	data, err := ioutil.ReadFile(input_file)
	if err != nil {
		fmt.Println("File reading error", err)
		return
	}
	// M4: Receives (key=name, value=null). Identity
	lines := strings.Split(string(data), "\n")
	lines = lines[:len(lines)-1]

	for _, line := range lines {
		fmt.Println(line)
	}

}
