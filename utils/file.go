package utils

import (
	"bufio"
	"os"
)

func GetStringListFromFile(path string) ([]string, error) {
	var stringList []string
	file, err := os.Open(path)
	if err != nil {
		return stringList, err
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		stringList = append(stringList, scanner.Text())
	}
	return stringList, nil
}
