package worker

import (
	"sync/atomic"
	"fmt"
	"time"
	"log"
)

func NewDispatcher(jobQueue chan Job, maxWorkers int, workFn func(job Job) error) *Dispatcher {
	workerPool := make(chan chan Job, maxWorkers)

	return &Dispatcher{
		workFn: workFn,
		jobQueue:   jobQueue,
		maxWorkers: maxWorkers,
		workerPool: workerPool,
	}
}

type Dispatcher struct {
	workerPool	chan chan Job
	maxWorkers	int
	jobQueue	chan Job
	workFn		func(job Job) error
	workers		[]*Worker
}

func (d *Dispatcher) Run() {
	for i := 0; i < d.maxWorkers; i++ {
		worker := NewWorker(i+1, d.workerPool, d.workFn)
		d.workers = append(d.workers, &worker)
		worker.start()
	}

	go d.dispatch()
}

func (d *Dispatcher) RunCount() int32 {
	var total int32 = 0

	for _, v := range d.workers {
		runningCount := v.GetRunningCount()
		final := atomic.LoadInt32(&runningCount)
		total += final
	}

	return total
}

func (d *Dispatcher) dispatch() {
	for {
		select {
		case job := <-d.jobQueue:
			go func() {
				fmt.Printf("fetching workerJobQueue for: %s\n", job.Name)
				workerJobQueue := <-d.workerPool
				fmt.Printf("adding %s to workerJobQueue\n", job.Name)
				workerJobQueue <- job
			}()
		}
	}
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

			// now that nothing is left, write tot he stop channel
			if runCount == 0 {
				stopChan <- true
			} else {
				log.Println("queued all jobs, but still running", runCount, "of them")
			}
		}
	}()

	// block until stop channel written
	<- stopChan
}