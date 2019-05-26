package batch

import (
	"encoding/xml"
	"errors"
	workers "github.com/markdicksonjr/go-worker"
	workersXml "github.com/markdicksonjr/go-worker/xml"
	"log"
	"strconv"
	"time"
)

type Reader struct {
	batch       *workers.Batch
	dispatcher  *workers.Dispatcher
	itemsToSave []interface{}
	jobName     string
	reader      workersXml.Reader
}

func (a *Reader) Init(
	jobName string,
	maxJobQueueSize int,
	maxWorkers int,
	batchSize int,
	workFn workers.WorkFunction, // what the worker will be doing (e.g. saving records), where job.Context is a.recordsToSave
	jobErrFn workers.JobErrorFunction,
	logFn workers.LogFunction,
) {
	a.jobName = jobName
	a.batch = &workers.Batch{}
	a.batch.Init(batchSize, func(i []interface{}) error {
		a.dispatcher.EnqueueJobAllowWait(workers.Job{Name: a.jobName, Context: workersXml.RecordArrayFromInterfaceArray(i, false)})
		return nil
	}, func(i []interface{}) error {
		a.dispatcher.EnqueueJobAllowWait(workers.Job{Name: a.jobName, Context: workersXml.RecordArrayFromInterfaceArray(i, true)})
		return nil
	})

	a.dispatcher = workers.NewDispatcher(maxJobQueueSize, maxWorkers, workFn, jobErrFn, logFn)
	a.dispatcher.Run()
}

func (a *Reader) Decode(
	filename string,
	recordsBuilder workersXml.RecordsBuilderFunction,
) error {
	if a.dispatcher == nil {
		return errors.New("decode called on batch reader before init")
	}

	a.reader = workersXml.Reader{}
	if err := a.reader.Open(filename); err != nil {
		return err
	}
	defer func() {
		if err := a.reader.Close(); err != nil {
			log.Println("an error occurred when closing file " + filename + ": " + err.Error())
		}
	}()

	batchCount := 0
	processedCount := 0
	for {

		// if the job queue isn't full, process an XML token
		if !a.dispatcher.IsJobQueueFull() {
			res := a.reader.BuildRecordsFromToken(recordsBuilder)

			// return any error that occurred
			if res.Err != nil {
				return res.Err
			}

			// if we got records from the token, push them into the batch
			if res.Records != nil && len(res.Records) > 0 {
				processedCount += len(res.Records)

				for _, v := range res.Records {
					if err := a.batch.Push(v); err != nil {
						return err
					}
				}
			}

			// if we've hit the end of the file, exit the for loop
			if res.IsEndOfStream {
				if err := a.batch.Flush(); err != nil {
					return err
				}
				break
			}

			// infinite loop guard for res.IsEndOfStream not firing TODO: remove this
			if batchCount%10000 == 0 && batchCount > 10000000 {
				if processedCount == 0 {
					if err := a.batch.Flush(); err != nil {
						return err
					}
					break
				} else {
					processedCount = 0
				}
			}
		} else {

			// the job queue is full, so wait a little bit to see if we can catch up a bit before continuing
			log.Println("waiting to read more XML, because the job queue is full")
			time.Sleep(time.Millisecond * 100)
		}
		batchCount++
	}

	// some jobs may be running still - wait until they're done
	a.dispatcher.WaitUntilIdle()

	utilization := a.dispatcher.GetUtilization()
	log.Println("the dispatcher had at least 1 worker active for " + strconv.FormatFloat(float64(utilization.PercentUtilization), 'f', 1, 32) + "% of the time")

	for _, u := range utilization.ByWorker {
		log.Println("worker " + strconv.Itoa(u.Id) + " was active " + strconv.FormatFloat(float64(u.PercentUtilization), 'f', 1, 32) + "% of the time")
	}

	return nil
}

func (a *Reader) DecodeToken(v interface{}, start *xml.StartElement) error {
	return a.reader.DecodeToken(v, start)
}

func (a *Reader) WaitUntilIdle() {
	a.dispatcher.WaitUntilIdle()
}
