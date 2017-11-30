package utils

func AllEqual(arr []string) bool {
	if len(arr) < 2 {
		return true
	}

	prev := arr[0]
	for i := 1; i < len(arr); i++ {
		if prev != arr[i] {
			return false
		}
		prev = arr[i]
	}
	return true
}
