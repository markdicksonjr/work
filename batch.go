package worker

import "errors"

type Batch struct {
	batchPosition	int
	batchSize		int
	itemsToSave		[]interface{}
}

type BatchHandler func([]interface{}) error

func (b *Batch) Init(batchSize int) {
	b.batchPosition = 0

	// grab the batch size - default to 100
	b.batchSize = batchSize
	if b.batchSize == 0 {
		b.batchSize = 100
	}
}

func (b *Batch) Push(record interface{}, onBatch BatchHandler) error {
	if b.batchSize == 0 {
		return errors.New("batch not initialized")
	}

	// allocate the buffer of items to save, if needed
	if b.itemsToSave == nil {
		newSlice := make([]interface{}, b.batchSize, b.batchSize)
		b.itemsToSave = newSlice
		b.batchPosition = 0
	}

	if b.batchPosition >= b.batchSize {
		batch := b.itemsToSave

		// allocate a new buffer, put the inbound record as the first item
		newSlice := make([]interface{}, b.batchSize, b.batchSize)
		b.itemsToSave = newSlice
		b.itemsToSave[0] = record
		b.batchPosition = 1
		if err := onBatch(batch); err != nil {
			return err
		}
	} else {
		b.itemsToSave[b.batchPosition] = record
		b.batchPosition++
	}

	return nil
}

func (b *Batch) Flush(onBatch BatchHandler) error {
	if b.batchSize == 0 {
		return errors.New("batch not initialized")
	}

	if len(b.itemsToSave) > 0 {
		subSlice := (b.itemsToSave)[0:b.batchPosition]
		b.itemsToSave = make([]interface{}, b.batchSize, b.batchSize)
		b.batchPosition = 0
		return onBatch(subSlice)
	}

	return nil
}