package worker

import (
	"sync/atomic"
	"time"
)

func NewDispatcher(maxJobQueueSize, maxWorkers int, workFn WorkFunction, jobErrFn JobErrorFunction, logFn LogFunction) *Dispatcher {
	return &Dispatcher{
		workFn:                  workFn,
		jobErrorFn:              jobErrFn,
		logFn:                   logFn,
		jobQueue:                make(chan Job, maxJobQueueSize),
		maxWorkers:              maxWorkers,
		workerPool:              make(chan chan Job, maxWorkers),
		idlenessSamplerInterval: 100 * time.Millisecond,
	}
}

type Utilization struct {
	ByWorker           []WorkerUtilization
	PercentUtilization float32
}

type WorkerUtilization struct {
	PercentUtilization float32
	Id                 int
}

type Dispatcher struct {
	workerPool chan chan Job
	maxWorkers int
	jobQueue   chan Job
	logFn      LogFunction
	jobErrorFn JobErrorFunction
	workFn     WorkFunction
	workers    []*Worker

	// idleness sampler properties
	idlenessSamplerStopChannel chan bool
	idlenessSamplerInterval    time.Duration
	idlenessQueriedIntervals   int64
	idlenessIntervals          int64
}

// start the dispatcher
// note that this will in no way block the app from proceeding
func (d *Dispatcher) Run() {
	for i := 0; i < d.maxWorkers; i++ {
		worker := NewWorker(i+1, d.workerPool, d.workFn, d.jobErrorFn, d.logFn)
		d.workers = append(d.workers, &worker)
		worker.start()
	}

	go d.dispatch()
	go d.sample()
}

// get the number of workers currently running
func (d *Dispatcher) RunCount() int32 {
	var total int32 = 0

	for _, v := range d.workers {
		runningCount := v.GetRunningCount()
		total += atomic.LoadInt32(&runningCount)
	}

	return total
}

// allows users to enqueue jobs into the work queue
func (d *Dispatcher) EnqueueJobAllowWait(job Job) {
	if blocked := d.BlockWhileQueueFull(); blocked {
		_, _ = d.logFn("blocked during enqueue because queue full")
	}
	d.jobQueue <- job
}

func (d *Dispatcher) EnqueueJobAllowDrop(job Job) {
	// TODO COUNT DROPS?
	d.jobQueue <- job
}

// simple check to see if the job queue is maxed out
func (d *Dispatcher) IsJobQueueFull() bool {
	return len(d.jobQueue) >= cap(d.jobQueue)
}

// blocks while the job queue is maxed out.  We don't want to drop the job, but we also don't want a constantly-growing
// queue ad infinitum
func (d *Dispatcher) BlockWhileQueueFull() bool {
	didBlock := false

	if d.IsJobQueueFull() {
		complete := make(chan bool)

		go func() {
			for d.IsJobQueueFull() {
				didBlock = true
				time.Sleep(time.Millisecond * 100)
			}
			complete <- true
		}()

		// wait until the complete channel is written to
		<-complete
	}

	return didBlock
}

// get the overall utilization for the dispatcher (all workers), as well as a summary of how effective each worker was at staying busy
func (d *Dispatcher) GetUtilization() Utilization {
	var results []WorkerUtilization
	for _, v := range d.workers {
		results = append(results, WorkerUtilization{
			PercentUtilization: v.GetPercentUtilization(),
			Id:                 v.id,
		})
	}

	return Utilization{
		PercentUtilization: 100.0 * (1 - float32(d.idlenessIntervals)/float32(d.idlenessQueriedIntervals)),
		ByWorker:           results,
	}
}

// blocks until all workers are idle, then resumes - typically, use this at the end of your flow to make sure all
// workers are done before proceeding or exiting
func (d *Dispatcher) WaitUntilIdle() {

	// allocate a channel
	stopChan := make(chan bool)

	// now that everything has been queued up, we want to make sure that we let everything
	// finish before we proceed through the app
	go func() {
		for {
			time.Sleep(250 * time.Millisecond)

			runCount := d.RunCount()

			// now that nothing is left, write to the stop channel
			if runCount == 0 {
				stopChan <- true
				return
			} else {
				_, _ = d.logFn("queued all jobs, but still running %d of them\n", runCount)
			}
		}
	}()

	// block until stop channel written
	<-stopChan
}

// pulls a job from the job queue and adds it to the worker's job queue - a worker will grab it in the worker logic
func (d *Dispatcher) dispatch() {
	for {

		// if there are no workers ready to receive the job, let the job queue fill up
		if !d.IsAnyWorkerIdle() {
			time.Sleep(30 * time.Millisecond)
			continue
		}

		_, _ = d.logFn("during round-robin enqueueing: %d running vs %d total\n", int(d.RunCount()), cap(d.workerPool))

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

// periodically check on the workers to get the runcount - if zero, add to the elapsed time count for "all workers idle"
func (d *Dispatcher) sample() {
	ticker := time.NewTicker(d.idlenessSamplerInterval)
	d.idlenessSamplerStopChannel = make(chan bool)
	go func() {
		for {
			select {
			case <-ticker.C:
				if d.RunCount() == 0 {
					d.idlenessIntervals++
				}
				d.idlenessQueriedIntervals++
			case <-d.idlenessSamplerStopChannel:
				ticker.Stop()
				return
			}
		}
	}()
}

func (d *Dispatcher) IsAnyWorkerIdle() bool {
	return int(d.RunCount()) < cap(d.workerPool)
}
