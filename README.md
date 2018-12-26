# Go-Worker

A simple worker pool, with a few utilities for specific use-cases for batch processing included.

## Job Queue Usage

```go

// start the dispatcher
maxWorkers := 8
dispatcher := worker.NewDispatcher(make(chan worker.Job, 100), maxWorkers, doWork, fmt.Printf)
dispatcher.Run()

// do something that loads up the jobs repeatedly (here, we use a for loop)
// for example, until EOF is reached while reading a file
for someCondition {
	
    // do some task to get something to put on the queue
    data, isEndOfStream, err := BogusDataSource(...)
    
    // put the thing on the queue
    dispatcher.EnqueueJob(worker.Job{Name: "address processing", Context: &data, IsEndOfStream: isEndOfStream})
}

// let all jobs finish before proceeding
dispatcher.WaitUntilIdle()
```

where doWork is something useful, but could be demonstrated with something like:

```go
func doWork(job worker.Job) error {
	time.Sleep(time.Second * 3)
	return nil
}
```

dispatcher.WaitUntilIdle() at the end of the first code block of the usage section will
wait until the workers are all waiting for work.  If any worker is occupied, the app will continue.  This
line is optional.  It's conceivable many apps will want to constantly operate the worker pool. No mechanism
to keep the app running indefinitely is provided in this library.

Another consideration is the case where Jobs get enqueued too quickly for the job queue.  One
mechanism is to check len(jobQueue) against cap(jobQueue) to see if it's full before inserting the job.
If it's full, the system should use time.Sleep and check again until there's space in the job queue.  An
example of this can be found in ./xml/batch/reader.go.

## Notes

The dispatcher uses round-robin dispatching.

worker.NoLogFunction (instead of fmt.Printf in the example above) is available in the event output is not desired.
