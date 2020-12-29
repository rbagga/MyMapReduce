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

	lines := strings.Split(string(data), "\n")
	lines = lines[:len(lines)-1]

	people := make(map[string][]int)

	for _, line := range lines {
		info := strings.Split(line, " ")

		key := strings.Split(info[0], "key=")[1]
		keys := strings.Split(key, ",")
		opp := keys[1] + "," + keys[0]

		values := strings.Split(info[1], "value=")[1]
		value, err := strconv.Atoi(values)
		if err != nil {
			// handle error
			fmt.Println(err)
		}

		if _, ok := people[key]; !ok {
			if _, ok := people[opp]; !ok {
				people[key] = append(people[key], 0, 0)
			} else {
				continue
			}
		}

		people[key][value] += 1
	}

	for key, val := range people {
		keys := strings.Split(key, ",")
		if val[1] > val[0] {
			fmt.Println("key=" + keys[0] + " value=" + keys[1])
		} else {
			fmt.Println("key=" + keys[1] + " value=" + keys[0])
		}
	}
}
