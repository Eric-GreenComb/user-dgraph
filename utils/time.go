package utils

import (
	"fmt"
	"time"
)

func GetTimeElapsed(start time.Time) time.Duration {
	current := time.Now()
	return current.Sub(start)
}

func PrintTimeElapsed(start time.Time, message string) {
	fmt.Println(message, GetTimeElapsed(start))
}
