package utils

func Includes[T comparable](obj T, lst []T) bool {
	for _, entry := range lst {
		if entry == obj {
			return true
		}
	}
	return false
}
