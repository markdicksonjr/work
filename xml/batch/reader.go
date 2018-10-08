package batch

import (
	"log"
	"time"
	"encoding/xml"
	workers "github.com/markdicksonjr/go-workers"
	workersXml "github.com/markdicksonjr/go-workers/xml"
)

type Reader struct {
	batchPosition	int
	batchSize		int
	dispatcher		*workers.Dispatcher
	itemsToSave		[]interface{}
	jobName 		string
	jobQueue		chan workers.Job
}

func (a *Reader) Init(
	jobName string,
	maxWorkers int,
	batchSize int,
	workFn workers.WorkFunction, // what the worker will be doing (e.g. saving records), where job.Context is a.recordsToSave
	logFn workers.LogFunction,
) {
	a.jobName = jobName
	a.batchSize = batchSize
	a.batchPosition = 0
	a.jobQueue = make(chan workers.Job, a.batchSize)
	a.dispatcher = workers.NewDispatcher(a.jobQueue, maxWorkers, workFn, logFn)
	a.dispatcher.Run()
}

func (a *Reader) BatchRecord(record interface{}) error {

	// grab the batch size - default to 100
	batchSize := a.batchSize
	if batchSize == 0 {
		batchSize = 100
	}

	// allocate the buffer of apps to save, if needed
	if a.itemsToSave == nil {
		newSlice := make([]interface{}, batchSize, batchSize)
		a.itemsToSave = newSlice
		a.batchPosition = 0
	}

	// if we have a full batch, queue up the job, otherwise, append
	if a.batchPosition >= batchSize {
		job := workers.Job{Name: a.jobName, Context: a.itemsToSave, IsEndOfStream: false}

		// allocate a new buffer
		newSlice := make([]interface{}, batchSize, batchSize)
		a.itemsToSave = newSlice
		a.itemsToSave[0] = record
		a.batchPosition = 1

		// queue up the job
		a.jobQueue <- job
	} else {
		a.itemsToSave[a.batchPosition] = record
		a.batchPosition++
	}
	return nil
}

// within processFn, you should call BatchRecord(obj) when you want to add a record to the batch
func (a *Reader) Decode(
	filename string,
	processFn func(t xml.Token) error,
) error {
	reader := workersXml.Reader{}
	err := reader.Open(filename)

	if err != nil {
		return err
	}

	isEof := false

	jobQueueCapacity := cap(a.jobQueue)

	for {

		// if the job queue isn't full, process an XML token
		if len(a.jobQueue) < jobQueueCapacity {
			isEof, err = reader.ProcessToken(processFn)

			if err != nil {
				return err
			}

			if isEof {
				a.onEndOfData()
				break
			}
		} else {

			// the job queue is full, so wait a little bit to see if we can catch up a bit before continuing
			log.Println("waiting to read more XML, because", jobQueueCapacity, "records are queued")
			time.Sleep(time.Millisecond * 250)
		}
	}

	// some jobs may be running still - wait until they're done
	a.dispatcher.WaitUntilIdle()

	return nil
}

func (a *Reader) onEndOfData() {

	// queue any remaining records
	if len(a.itemsToSave) > 0 {
		subSlice := (a.itemsToSave)[0:a.batchPosition]
		job := workers.Job{Name: a.jobName, Context: &subSlice, IsEndOfStream: true}
		a.jobQueue <- job
	}
}

