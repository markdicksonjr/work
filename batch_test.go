package work

import (
	"errors"
	"strconv"
	"testing"
)

func TestBatch_PushSingleItemBatch(t *testing.T) {
	b1 := Batch{}
	singleItemBatchCallCount := 0
	b1.Init(1, func(i []interface{}) error {
		singleItemBatchCallCount++
		if len(i) != 1 {
			return errors.New("size did not match for i = " + strconv.Itoa(singleItemBatchCallCount))
		}
		if iAsInt, ok := i[0].(int); !ok {
			return errors.New("payload was not of correct type")
		} else if iAsInt != singleItemBatchCallCount {
			return errors.New("payload incorrect and likely arrived out of order")
		}
		return nil
	})

	if err := b1.Push(1); err != nil {
		t.Fatal(err)
	}
	if err := b1.Push(2); err != nil {
		t.Fatal(err)
	}

	// flush should yield no call
	if err := b1.Flush(); err != nil {
		t.Fatal(err)
	}

	if singleItemBatchCallCount != 2 {
		t.Fail()
	}
}


func TestBatch_Push(t *testing.T) {
	b2 := Batch{}
	itemBatchCallCount := 0
	b2.Init(2, func(i []interface{}) error {
		itemBatchCallCount++
		if len(i) > 2 {
			return errors.New("batch size too large for i = " + strconv.Itoa(itemBatchCallCount))
		}

		var i0, i1 int
		var ok bool

		// grab the first item
		if i0, ok = i[0].(int); !ok {
			return errors.New("payload was not of correct type")
		}

		// grab the second item, if it's there
		if len(i) > 1 {
			if i1, ok = i[1].(int); !ok {
				return errors.New("payload was not of correct type")
			}
		} else {
			if itemBatchCallCount != 3 {
				return errors.New("single item batch occurred at the wrong time")
			}
			return nil
		}

		if i0 >= i1 {
			return errors.New("payload was out of order")
		}

		if i0 > itemBatchCallCount * 2 || i1 > itemBatchCallCount * 2 {
			return errors.New("payload was out of order - result escalated too quickly")
		}

		return nil
	})

	if err := b2.Push(1); err != nil {
		t.Fatal(err)
	}
	if err := b2.Push(2); err != nil {
		t.Fatal(err)
	}
	if err := b2.Push(3); err != nil {
		t.Fatal(err)
	}
	if err := b2.Push(4); err != nil {
		t.Fatal(err)
	}
	if err := b2.Push(4); err != nil {
		t.Fatal(err)
	}

	// flush should yield a call with a single item
	if err := b2.Flush(); err != nil {
		t.Fatal(err)
	}

	if itemBatchCallCount != 3 {
		t.Fail()
	}
}
