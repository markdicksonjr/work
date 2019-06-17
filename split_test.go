package work

import "testing"

func TestSplit(t *testing.T) {
	val := []interface{} { 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12 }
	res := Split(val, 5)
	if len(res) != 5 {
		t.Fatal("the number of parts was not 5")
	}

	existsMap := make(map[int]bool)
	sizeSum := 0
	for _, v := range res {
		sizeSum += len(v)

		for _, i := range v {
			existsMap[i.(int)] = true
		}
	}

	if sizeSum != 12 {
		t.Fatal("the size of the result was not 12")
	}

	if len(existsMap) != 12 {
		t.Fatal("there were not 12 unique values, as expected")
	}
}
