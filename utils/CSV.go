package utils

import "strconv"

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
