package utils

import (
	"fmt"
	"testing"
)

func TestHashSetToCSVString(t *testing.T) {
	m := make(map[string]string)
	m["asd"] = "as"
	m["ad"] = "asd"
	m["ge"] = "SAd"
	//fmt.Println(HashSetStringToCSVString(m))

	if m["gk"] == "" {
		fmt.Println("right")
	}
}
