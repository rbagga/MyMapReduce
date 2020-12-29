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

	votes := strings.Split(string(data), "\n")

	m := 3
	for _, vote := range votes {
		vote = strings.ReplaceAll(vote, " ", "")

		for i := 0; i < m-1; i++ {
			for j := i + 1; j < m; j++ {
				if vote[i] < vote[j] {
					fmt.Println("key=" + string(vote[i]) + "," + string(vote[j]) + " value=1")
				} else {
					fmt.Println("key=" + string(vote[i]) + "," + string(vote[j]) + " value=0")
				}
			}
		}
	}

}
