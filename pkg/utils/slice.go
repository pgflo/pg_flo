package utils

// SlicesEqual compares two slices of any comparable type, ignoring the order of elements.
func SlicesEqual[T comparable](slice1, slice2 []T) bool {
	if len(slice1) != len(slice2) {
		return false
	}
	map1 := make(map[T]int, len(slice1))
	map2 := make(map[T]int, len(slice2))

	for _, val := range slice1 {
		map1[val]++
	}
	for _, val := range slice2 {
		map2[val]++
	}

	for key, val := range map1 {
		if map2[key] != val {
			return false
		}
	}
	return true
}
