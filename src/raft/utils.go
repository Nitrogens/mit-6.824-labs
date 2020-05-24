package raft

func LowerBound(array []LogEntry, target int) int {
	left := 0
	right := len(array) - 1
	for left < right {
		mid := (left + right) >> 1
		if array[mid].Term >= target {
			right = mid
		} else {
			left = mid + 1
		}
	}
	return right
}

func UpperBound(array []LogEntry, target int) int {
	left := 0
	right := len(array) - 1
	for left < right {
		mid := (left + right) >> 1
		if array[mid].Term > target {
			right = mid
		} else {
			left = mid + 1
		}
	}
	return right
}