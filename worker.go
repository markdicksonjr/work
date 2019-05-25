package worker

import (
	"sync/atomic"
	"time"
)

type Job struct {
	Name    string
	Context interface{}
}

// provides a mechanism for shared data and context across calls to the work function
type Context struct {
	Data interface{}
}

func NewWorker(id int, workerPool chan chan Job, workFn WorkFunction, jobErrorFn JobErrorFunction, logFn LogFunction) Worker {
	return Worker{
		id:            id,
		jobQueue:      make(chan Job),
		workerPool:    workerPool,
		quitChan:      make(chan bool),
		workFn:        workFn,
		jobErrorFn:    jobErrorFn,
		logFn:         logFn,
		workerContext: Context{},
	}
}

type Worker struct {
	id                    int
	jobQueue              chan Job
	workerPool            chan chan Job
	quitChan              chan bool
	workFn                WorkFunction
	jobErrorFn            JobErrorFunction
	runningCount          int32
	startTime             time.Time
	totalProcessingTimeNs int64
	logFn                 LogFunction
	workerContext         Context
}

func (w Worker) GetRunningCount() int32 {
	return w.runningCount
}

// time since started
func (w Worker) GetRunTimeNs() int64 {
	return time.Now().Sub(w.startTime).Nanoseconds()
}

// how long the worker spent doing things across all runs
func (w Worker) GetTotalActiveTimeNs() int64 {
	return w.totalProcessingTimeNs
}

// how long the worker spent doing nothing across all runs
func (w Worker) GetTotalIdleTimeNs() int64 {
	return time.Now().Sub(w.startTime).Nanoseconds() - w.GetTotalActiveTimeNs()
}

// how much of the time the worker spent doing things across all runs, by %
func (w Worker) GetPercentUtilization() float32 {
	if w.GetRunTimeNs() == 0 {
		return 0.0
	}
	return 100.0 * float32(w.GetTotalActiveTimeNs()) / float32(w.GetRunTimeNs())
}

func (w *Worker) start() {
	w.startTime = time.Now()

	go func() {
		for {
			w.workerPool <- w.jobQueue

			select {
			case job := <-w.jobQueue:
				workFnStart := time.Now()
				atomic.AddInt32(&w.runningCount, 1)
				_, _ = w.logFn("worker%d: started %s\n", w.id, job.Name)
				err := w.workFn(job, &w.workerContext)
				atomic.AddInt32(&w.runningCount, -1)
				atomic.AddInt64(&w.totalProcessingTimeNs, time.Now().Sub(workFnStart).Nanoseconds())

				if err != nil {
					_, _ = w.logFn("worker%d: had error in %s: %s!\n", w.id, job.Name, err.Error())
					w.jobErrorFn(job, &w.workerContext, err)
				}

				// nil out data to clue GC
				w.workerContext.Data = nil

				_, _ = w.logFn("worker%d: completed %s!\n", w.id, job.Name)
			case <-w.quitChan:
				_, _ = w.logFn("worker%d stopping\n", w.id)
				return
			}
		}
	}()
}

func (w Worker) stop() {
	go func() {
		w.quitChan <- true
	}()
}
