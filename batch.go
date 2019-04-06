package worker

import "errors"

type Batch struct {
	batchPosition int
	batchSize     int
	itemsToSave   []interface{}
	pushHandler   BatchHandler
	flushHandler  BatchHandler
}

type BatchHandler func([]interface{}) error

func (b *Batch) Init(batchSize int, pushHandler BatchHandler, flushHandler BatchHandler) {
	b.batchPosition = 0

	// grab the batch size - default to 100
	b.batchSize = batchSize
	if b.batchSize == 0 {
		b.batchSize = 100
	}

	b.pushHandler = pushHandler
	b.flushHandler = flushHandler
}

func (b *Batch) Push(record interface{}) error {
	if b.batchSize == 0 {
		return errors.New("batch not initialized")
	}

	// allocate the buffer of items to save, if needed
	if b.itemsToSave == nil {
		b.itemsToSave = make([]interface{}, b.batchSize, b.batchSize)
		b.batchPosition = 0
	}

	if b.batchPosition >= b.batchSize {
		batch := b.itemsToSave

		// allocate a new buffer, put the inbound record as the first item
		b.itemsToSave = make([]interface{}, b.batchSize, b.batchSize)
		b.itemsToSave[0] = record
		b.batchPosition = 1
		if err := b.pushHandler(batch); err != nil {
			return err
		}
	} else {
		b.itemsToSave[b.batchPosition] = record
		b.batchPosition++
	}

	return nil
}

func (b *Batch) Flush() error {
	if b.batchSize == 0 {
		return errors.New("batch not initialized")
	}

	if len(b.itemsToSave) > 0 {
		subSlice := (b.itemsToSave)[0:b.batchPosition]
		b.itemsToSave = make([]interface{}, b.batchSize, b.batchSize)
		b.batchPosition = 0
		return b.flushHandler(subSlice)
	}

	return nil
}
