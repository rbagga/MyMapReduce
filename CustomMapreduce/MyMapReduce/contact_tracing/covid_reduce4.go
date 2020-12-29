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
	// R4: Receives (key=name, value=(null, null,...)) (multiple possible entries).
	// Emit(key=name, value=needs-to-be-tested).
	lines := strings.Split(string(data), "\n")
	lines = lines[:len(lines)-1]

	set := make(map[string]struct{})
	var exists = struct{}{}

	for _, line := range lines {
		info := strings.Split(line, " ")
		key := strings.Split(info[0], "key=")[1]
		set[key] = exists
	}

	for name, _ := range set {
		fmt.Println("key=" + name + " value=needs-to-be-tested")
	}
}
