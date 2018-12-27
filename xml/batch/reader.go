package batch

import (
	"encoding/xml"
	"errors"
	workers "github.com/markdicksonjr/go-worker"
	workersXml "github.com/markdicksonjr/go-worker/xml"
	"log"
	"time"
)

type Reader struct {
	batchPosition	int
	batchSize		int
	dispatcher		*workers.Dispatcher
	itemsToSave		[]interface{}
	jobName 		string
	reader 			workersXml.Reader
}

func (a *Reader) Init(
	jobName string,
	maxJobQueueSize int,
	maxWorkers int,
	batchSize int,
	workFn workers.WorkFunction, // what the worker will be doing (e.g. saving records), where job.Context is a.recordsToSave
	logFn workers.LogFunction,
) {
	a.jobName = jobName
	a.batchSize = batchSize
	a.batchPosition = 0
	a.dispatcher = workers.NewDispatcher(maxJobQueueSize, maxWorkers, workFn, logFn)
	a.dispatcher.Run()
}

func (a *Reader) BatchRecord(record interface{}) error {

	if a.dispatcher == nil {
		return errors.New("batchRecord called on batch reader before init")
	}

	// grab the batch size - default to 100
	batchSize := a.batchSize
	if batchSize == 0 {
		batchSize = 100
	}

	// allocate the buffer of items to save, if needed
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
		a.dispatcher.EnqueueJob(job)
	} else {
		a.itemsToSave[a.batchPosition] = record
		a.batchPosition++
	}
	return nil
}

func (a *Reader) Decode(
	filename string,
	recordsBuilder workersXml.RecordsBuilderFunction,
) error {

	if a.dispatcher == nil {
		return errors.New("decode called on batch reader before init")
	}

	a.reader = workersXml.Reader{}
	err := a.reader.Open(filename)

	if err != nil {
		return err
	}

	for {

		// if the job queue isn't full, process an XML token
		if !a.dispatcher.IsJobQueueFull() {
			res := a.reader.BuildRecordsFromToken(recordsBuilder)

			if res.Records != nil && len(res.Records) > 0 {
				for _, v := range res.Records {
					if err := a.BatchRecord(v); err != nil {
						return err
					}
				}
			}

			if res.IsEndOfStream {

				// queue any remaining records into the job queue (flush)
				if len(a.itemsToSave) > 0 {
					subSlice := (a.itemsToSave)[0:a.batchPosition]
					job := workers.Job{Name: a.jobName, Context: subSlice, IsEndOfStream: true}
					a.dispatcher.EnqueueJob(job)
				}
				break
			}
		} else {

			// the job queue is full, so wait a little bit to see if we can catch up a bit before continuing
			log.Println("waiting to read more XML, because the job queue is full")
			time.Sleep(time.Millisecond * 100)
		}
	}

	// some jobs may be running still - wait until they're done
	a.dispatcher.WaitUntilIdle()

	return nil
}

func (a *Reader) DecodeToken(v interface{}, start *xml.StartElement) error {
	return a.reader.DecodeToken(v, start)
}

func (a *Reader) WaitUntilIdle() {
	a.dispatcher.WaitUntilIdle()
}