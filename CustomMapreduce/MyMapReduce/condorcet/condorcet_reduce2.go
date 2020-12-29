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

	people := make(map[string]int)

	m := 3

	maxcount := 0

	for _, line := range lines {
		info := strings.Split(line, " ")

		val := strings.Split(info[1], "value=")[1]

		values := strings.Split(val, ",")

		people[values[0]] += 1

		if people[values[0]] == m-1 {
			fmt.Println("key=" + values[0] + " value=Condorcet winner!")
			return
		}

		if people[values[0]] > maxcount {
			maxcount = people[values[0]]
		}
	}

	set := make(map[string]struct{})
	var exists = struct{}{}

	for key, val := range people {
		if val == maxcount {
			set[key] = exists
		}
	}

	for key, _ := range set {
		fmt.Println("key=" + key + " value=No Condorcet winner, Highest Condorcet counts")
	}
}
