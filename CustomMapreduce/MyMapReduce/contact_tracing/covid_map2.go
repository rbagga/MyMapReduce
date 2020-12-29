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
	// M2 reads from D2 and outputs (key=name, value = positive)
	lines := strings.Split(string(data), "\n")

	for _, line := range lines {
		fmt.Println("key=" + line + " value=positive")
	}

}
