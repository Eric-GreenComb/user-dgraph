package utils

import (
	"log"
	"sort"
	"testing"
)

func TestAllEqual(t *testing.T) {
	var arr []string
	arr = append(arr, "81234561")
	arr = append(arr, "8123456")
	arr = append(arr, "81234567")

	log.Println(AllEqual(arr))
	log.Println(arr)
	sort.Strings(arr)
	log.Println(arr)

}
