package work

import "math"

// Split will make "parts" evenly-sized slices, with the last slice smaller than the others in the event len(data) is
// not divisible by parts
func Split(data []interface{}, parts int) [][]interface{} {
	if parts < 2 {
		return [][]interface{}{ data }
	}

	// compute how large parts are
	subLen := math.Floor(float64(len(data) / parts))

	// build the returned result
	results := make([][]interface{}, parts)

	// loop through data and slice out the evenly-sized parts
	for i := 0; i < parts - 1; i++ {
		results[i] = data[int(subLen) * i:int(subLen) * i + int(subLen)]
	}

	// grab the end, which is the remainder-sized slice
	results[parts - 1] = data[(parts - 1) * int(subLen):]

	return results
}
