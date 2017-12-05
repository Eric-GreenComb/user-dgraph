package utils

import (
	"log"
	"sort"
	"testing"
)

func TestAllEqual(t *testing.T) {
	/*var arr []string
	arr = append(arr, "81234561")
	arr = append(arr, "8123456")
	arr = append(arr, "81234567")

	log.Println(AllEqual(arr))*/

	oldPhoneNos := []string{"087780132215", "0895320494434"}
	newPhoneNos := []string{"087780132215", "0895320494434"}

	if len(oldPhoneNos) != len(newPhoneNos) {
		log.Println("Return New")
	}

	sort.Strings(oldPhoneNos)
	sort.Strings(newPhoneNos)

	allEquals := true
	for i, _ := range newPhoneNos {
		if newPhoneNos[i] != oldPhoneNos[i] {
			allEquals = false
			break
		}
	}

	if allEquals {
		log.Println("DontReturn New")
	} else {
		log.Println("Return New")
	}

}
