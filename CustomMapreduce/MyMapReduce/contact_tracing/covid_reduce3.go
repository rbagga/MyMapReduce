package main

import (
	"fmt"
	"io/ioutil"
	"os"
	"strconv"
	"strings"
)

func main() {
	input_file := os.Args[1]

	data, err := ioutil.ReadFile(input_file)
	if err != nil {
		fmt.Println("File reading error", err)
		return
	}
	//
	lines := strings.Split(string(data), "\n")
	lines = lines[:len(lines)-1]
	locations := make(map[string][]string)
	positives := make(map[string]string)

	for _, line := range lines {
		info := strings.Split(line, " ")
		loc := strings.Split(info[0], "key=")[1]
		val := strings.Split(info[1], "value=")[1]
		values := strings.Split(val, ",")

		if values[0] == "positive" {
			positives[loc] = val
		}
		locations[loc] = append(locations[loc], val)
	}

	for loc, val := range positives {
		values := strings.Split(val, ",")
		covid_start, err := strconv.Atoi(values[1])
		if err != nil {
			// handle error
			fmt.Println(err)
		}

		covid_end, err := strconv.Atoi(values[2])
		if err != nil {
			// handle error
			fmt.Println(err)
		}
		for _, person := range locations[loc] {
			info := strings.Split(person, ",")
			start, err := strconv.Atoi(info[1])
			if err != nil {
				// handle error
				fmt.Println(err)
			}

			end, err := strconv.Atoi(info[2])
			if err != nil {
				// handle error
				fmt.Println(err)
			}

			if start < covid_end || covid_start < end {
				fmt.Println("key=" + info[3] + " value=null")
			}
		}
	}
}
