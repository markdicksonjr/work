package worker

import (
	"sync/atomic"
	"time"
)

func NewDispatcher(maxJobQueueSize, maxWorkers int, workFn WorkFunction, logFn LogFunction) *Dispatcher {
	workerPool := make(chan chan Job, maxWorkers)

	return &Dispatcher{
		workFn: workFn,
		logFn: logFn,
		jobQueue:   make(chan Job, maxJobQueueSize),
		maxWorkers: maxWorkers,
		workerPool: workerPool,
	}
}

type Dispatcher struct {
	workerPool	chan chan Job
	maxWorkers	int
	jobQueue	chan Job
	logFn		LogFunction
	workFn		WorkFunction
	workers		[]*Worker
}

func (d *Dispatcher) Run() {
	for i := 0; i < d.maxWorkers; i++ {
		worker := NewWorker(i+1, d.workerPool, d.workFn, d.logFn)
		d.workers = append(d.workers, &worker)
		worker.start()
	}

	go d.dispatch()
}

func (d *Dispatcher) RunCount() int32 {
	var total int32 = 0

	for _, v := range d.workers {
		runningCount := v.GetRunningCount()
		total += atomic.LoadInt32(&runningCount)
	}

	return total
}

func (d *Dispatcher) EnqueueJob(job Job) {
	d.jobQueue <- job
}

func (d *Dispatcher) IsJobQueueFull() bool {
	return len(d.jobQueue) >= cap(d.jobQueue);
}

// blocks until all workers are idle, then results
func (d *Dispatcher) WaitUntilIdle() {

	// allocate a channel
	stopChan := make(chan bool)

	// now that everything has been queued up, we want to make sure that we let everything
	// finish before we proceed through the app
	go func() {
		for {
			time.Sleep(time.Second)

			runCount := d.RunCount()

			// now that nothing is left, write to the stop channel
			if runCount == 0 {
				stopChan <- true
			} else {
				_, _ = d.logFn("queued all jobs, but still running %d of them\n", runCount)
			}
		}
	}()

	// block until stop channel written
	<- stopChan
}

func (d *Dispatcher) dispatch() {
	for {
		select {
		case job := <-d.jobQueue:
			go func() {
				_, _ = d.logFn("fetching workerJobQueue for: %s\n", job.Name)
				workerJobQueue := <-d.workerPool
				_, _ = d.logFn("adding %s to workerJobQueue\n", job.Name)
				workerJobQueue <- job
			}()
		}
	}
}
