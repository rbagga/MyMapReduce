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

	people := make(map[string][]string)

	// If value contains “positive”, then emit (key=location, value=(positive, (start, end)))
	// If value is (location, start, end), then emit (key=location, value=(testcase,(start, end, name)))
	lines := strings.Split(string(data), "\n")
	lines = lines[:len(lines)-1]

	for _, line := range lines {
		info := strings.Split(line, " ")

		key := strings.Split(info[0], "key=")[1]

		value := strings.Split(info[1], "value=")[1]

		if _, ok := people[key]; !ok {
			people[key] = append(people[key], "", "")
		}

		if value == "positive" {
			people[key][1] = value
		} else {
			people[key][0] = value
		}
	}

	for key, vals := range people {
		info := strings.Split(vals[0], ",")
		if vals[1] == "positive" {
			fmt.Println("key=" + info[0] + " value=positive," + info[1] + "," + info[2])
		} else {
			fmt.Println("key=" + info[0] + " value=testcase," + info[1] + "," + info[2] + "," + key)
		}

	}

}
