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
	// M1 reads from D1 and outputs (key=name, value=(location, start, end))
	lines := strings.Split(string(data), "\n")

	for _, line := range lines {
		info := strings.Split(line, " ")
		fmt.Println("key=" + info[0] + " value=" + info[1] + "," + info[2] + "," + info[3])
	}

}
