package work

import (
	"errors"
	"sync"
)

type Batch struct {
	batchPosition int
	batchSize     int
	itemsToSave   []interface{}
	pushHandler   BatchHandler
	flushHandler  BatchHandler
	mutex         *sync.Mutex
}

// convenience interface - not used directly by this module
type BatchSource interface {

	// when the caller wants to process slices of data
	// gives the batch and some context about where in the whole set
	GetBatches(
		onBatch func(batch []interface{}, batchIndex, batchSize, totalItemCount int) error,
	) error
}


// convenience interface - not used directly by this module
type BatchDestination interface {
	PutBatch([]interface{}) error
	Finalize() error
}

// convenience interface - not used directly by this module
type BytesSource interface {

	// when the caller wants to process bytes of data per batch
	// gives the batch and some context about where in the whole set
	GetBatches(
		onBatch func(bytes []byte, batchIndex, batchSize, totalItemCount int) error,
	) error
}

// note: io.WriteCloser makes a convenient alternative to "BytesDestination"

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
	b.mutex = &sync.Mutex{}
}

func (b *Batch) Push(record interface{}) error {
	if b.batchSize == 0 {
		return errors.New("batch not initialized")
	}

	// lock around batch processing
	b.mutex.Lock()

	// allocate the buffer of items to save, if needed
	if b.itemsToSave == nil {
		b.itemsToSave = make([]interface{}, b.batchSize, b.batchSize)
		b.itemsToSave[0] = record
		b.batchPosition = 1
		b.mutex.Unlock()
	} else if b.batchPosition >= b.batchSize {
		batch := b.itemsToSave

		// allocate a new buffer, put the inbound record as the first item
		b.itemsToSave = make([]interface{}, b.batchSize, b.batchSize)
		b.itemsToSave[0] = record
		b.batchPosition = 1

		// release the lock
		b.mutex.Unlock()

		// TODO: review impact of making this call from a goroutine - definitely faster, but would bugs arise from timing changes?
		if err := b.pushHandler(batch); err != nil {
			return err
		}

		// dereference batch to clue GC, unless user wants to retain data
		batch = nil
	} else {
		b.itemsToSave[b.batchPosition] = record
		b.batchPosition++
		b.mutex.Unlock()
	}

	return nil
}

func (b *Batch) GetPosition() int {
	b.mutex.Lock()
	pos := b.batchPosition
	b.mutex.Unlock()
	return pos
}

func (b *Batch) Flush() error {
	if b.batchSize == 0 {
		return errors.New("batch not initialized")
	}

	// lock around batch processing
	b.mutex.Lock()
	if b.batchPosition > 0 {

		// snag the rest of the buffer as a slice, reset buffer
		subSlice := (b.itemsToSave)[0:b.batchPosition]
		b.itemsToSave = make([]interface{}, b.batchSize, b.batchSize)
		b.batchPosition = 0

		// we've finished batch processing, unlock
		b.mutex.Unlock()

		// call the configured flush handler
		err := b.flushHandler(subSlice)
		subSlice = nil
		return err
	}
	b.mutex.Unlock()

	return nil
}
