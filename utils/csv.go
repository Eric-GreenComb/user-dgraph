package utils

import (
	"fmt"
	"strconv"
)

func SliceToCSV(list []int64) string {
	output := ""
	for _, v := range list {
		output += strconv.FormatInt(v, 10) + ","
	}
	output = output[:len(output)-1]
	return output
}

func HashSetToCSV(hashset map[int64]bool) string {
	output := ""
	for k, _ := range hashset {
		output += strconv.FormatInt(k, 10) + ","
	}
	output = output[:len(output)-1]
	return output
}

func HashSetIntToCSVString(hashset map[int64]bool) string {
	output := ""
	for k, _ := range hashset {
		output += fmt.Sprintf(`"%d",`, k)
	}
	output = output[:len(output)-1]
	return output
}

func HashSetStringToCSVString(hashset map[string]bool) string {
	output := ""
	for k, _ := range hashset {
		output += fmt.Sprintf(`"%s",`, k)
	}
	output = output[:len(output)-1]
	return output
}

func StringListToCSVString(stringlist []string) string {
	output := ""
	for _, v := range stringlist {
		output += fmt.Sprintf(`"%v",`, v)
	}
	output = output[:len(output)-1]
	return output
}
