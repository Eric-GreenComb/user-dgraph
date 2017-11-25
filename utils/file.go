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

//Write the main-toComp into dest (ignore if toComp's extra content)
func WriteDiff(main, toComp, dest string) error {
	//open main, tocomp to read
	mainF, err := os.Open(main)
	if err != nil {
		return err
	}
	defer mainF.Close()

	toCompF, err := os.Open(toComp)
	if err != nil {
		return err
	}
	defer toCompF.Close()

	//open/overwrite destF to write
	destF, err := os.OpenFile(dest, os.O_CREATE|os.O_WRONLY, 0666)
	if err != nil {
		return err
	}
	defer destF.Close()

	toCompFMap := make(map[string]bool)
	scanner := bufio.NewScanner(toCompF)

	for scanner.Scan() {
		toCompFMap[scanner.Text()] = true
	}

	scanner = bufio.NewScanner(mainF)
	for scanner.Scan() {
		text := scanner.Text()
		if !toCompFMap[text] {
			_, err := destF.WriteString(text + "\n")
			if err != nil {
				return err
			}
		}
	}

	return nil
}
