# Work (for Go)

A worker pool and batch processing library.  Ships with a few utilities for common use-cases (e.g. XML processing).

## Job Queue Usage

```go

// start the dispatcher, using 8 workers and up to 100 queued jobs
maxWorkers := 8
maxJobQueueSize := 100
dispatcher := work.NewDispatcher(maxJobQueueSize, maxWorkers, doWork, fmt.Printf)
dispatcher.Run()

// do something that loads up the jobs repeatedly (here, we use a for loop)
// for example, until EOF is reached while reading a file
for someCondition {
	
    // do some task to get something to put on the queue
    data, isEndOfStream, err := BogusDataSource(...)
    
    // ensure the queue doesn't drop the item
    dispatcher.BlockWhileQueueFull()
    
    // put the thing on the queue
    dispatcher.EnqueueJob(work.Job{Name: "address processing", Context: &data})
}

// let all jobs finish before proceeding
dispatcher.WaitUntilIdle()
```

where doWork is something useful, but could be demonstrated with something like:

```go
func doWork(job work.Job, ctx *work.Context) error {
	time.Sleep(time.Second * 3)
	return nil
}
```

dispatcher.WaitUntilIdle() at the end of the first code block of the usage section will
wait until the workers are all waiting for work.  If any worker is occupied, the app will continue.  This
line is optional.  It's conceivable many apps will want to constantly operate the worker pool. No mechanism
to keep the app running indefinitely is provided in this library, as it's pretty easy with core Go to do 
(channels among other things, make this particularly trivial).

Another consideration is the case where Jobs get enqueued too quickly for the job queue.  One
strategy to use for this is to call Dispatcher.IsJobQueueFull to see if the job queue is full before inserting a job.
If it's full, the app should wait a bit and check again until there's space in the job queue.  An
example of this can be found in ./xml/batch/reader.go.

## Utilities

### Batch

A general-use batching mechanism exists in this library.  Any time you need to add a bunch of records (especially one 
at a time) and process them as a big group, this will be a handy utility.  A simple usage example:

```go
batch := &work.Batch{}
batch.Init(a.BatchSize, onPush, onFlush)

...

// for each item that is read, add it to the batch
if err := batch.Push(item); err != nil {
    return err
}
```

Where the push handler (called when a batch is large enough to dequeue, or is flushed) might look something like:

```go
handler := func(i []interface{}) error {
    var mapStrings []map[string]string = nil
    
    // loop through the whole batch, casting each to the
    // correct type from interface{}
    for _, item := range i {
        itemString := item.(map[string]string)
        mapStrings = append(mapStrings, itemString)
    }

    // save the data somewhere
    return persister.Put(mapStrings)
}
```

After all batches are processed, the user should call flush to ensure the final batch gets processed, as the final batch
will likely not have been large enough to trigger the push handler.

A flush handler is basically the same thing as the push handler, except it should be in the context of finalizing any 
state your app may have, and it will contain less than a full batch of records.  In most cases, the same function can be 
passed for both arguments.

### MutexFunction

A function that will only ever be run at most once at any given time.

### XML

A sample integration of Batch and and XML Reader is provided in ./xml/sample.

### Simple Timer

To create a timer, call `NewTimer()` (optionally, you can set timer.NoOp to disable the timer processing (if not debugging, etc).

To use the timer, call `timer.Start("SomeTitle")` where SomeTitle describes the operation that is
being timed.  When complete, call `timer.Stop("SomeTitle")` where SomeTitle is the same operation
description used for the corresponding timer Start.

You can output the timings with simple code like this:

```go
for _, timing := range timer.GetTimings() {
    log.Println("Running totals for " + timing.Label + ": sum = " + strconv.Itoa(int(timing.TotalTime / 1000000)) + "ms, avg = " + strconv.Itoa(int(timing.TotalTime / 1000000) / timing.Count) + "ms")
}
```

If you want to reduce the timer overhead significantly by shutting it off, set "NoOp" to true when initializing the
timer.

## Notes

The dispatcher uses round-robin dispatching.

work.NoLogFunction (instead of fmt.Printf in the example above) is available in the event output is not desired.
