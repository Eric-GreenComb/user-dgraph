package utils

import "strings"

func NormalizePhone(s string) string {
	dphone := strings.Replace(s, "-", "", -1)
	dphone = strings.Replace(dphone, "+", "", -1)
	if strings.HasPrefix(dphone, "0") {
		dphone = dphone[1:]
	}

	if strings.HasPrefix(dphone, "62") {
		dphone = dphone[2:]
	}
	return dphone
}
