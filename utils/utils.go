package utils

import "time"

func WaitUntil(duration time.Duration) {
	nextTime := time.Now().Truncate(duration)
	nextTime = nextTime.Add(duration)
	time.Sleep(time.Until(nextTime))
}
