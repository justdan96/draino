package utils

// MapValues will return all values from the given map
func MapValues[T comparable, V any](m map[T]V) []V {
	res := make([]V, 0, len(m))
	for _, val := range m {
		res = append(res, val)
	}
	return res
}
