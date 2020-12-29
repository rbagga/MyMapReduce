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

	lines := strings.Split(string(data), "\n")
	lines = lines[:len(lines)-1]

	for _, line := range lines {
		info := strings.Split(line, " ")
		key := strings.Split(info[0], "key=")[1]
		value := strings.Split(info[1], "value=")[1]

		fmt.Println("key=1" + " value=" + key + "," + value)
	}

}
