package work

import (
	"sync/atomic"
	"time"
)

type Job struct {
	Context interface{}
}

// provides a mechanism for shared data and context across calls to the work function
type Context struct {
	Data interface{}
	Id   int
}

func NewWorker(id int, workerPool chan chan Job, workFn WorkFunction, jobErrorFn JobErrorFunction, logFn LogFunction) Worker {
	return Worker{
		jobQueue:   make(chan Job),
		workerPool: workerPool,
		quitChan:   make(chan bool),
		workFn:     workFn,
		jobErrorFn: jobErrorFn,
		logFn:      logFn,
		workerContext: Context{
			Id: id,
		},
	}
}

type Worker struct {
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
				_, _ = w.log("worker%d: started job", w.workerContext.Id)
				err := w.workFn(job, &w.workerContext)
				atomic.AddInt32(&w.runningCount, -1)
				atomic.AddInt64(&w.totalProcessingTimeNs, time.Now().Sub(workFnStart).Nanoseconds())

				if err != nil {
					_, _ = w.log("worker%d: had error: %s", w.workerContext.Id, err.Error())
					w.error(job, &w.workerContext, err)
				}

				// nil out data to clue GC
				job.Context = nil

				_, _ = w.log("worker%d: completed job", w.workerContext.Id)
			case <-w.quitChan:
				_, _ = w.log("worker%d stopping", w.workerContext.Id)
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

func (w Worker) log(format string, a ...interface{}) (n int, err error) {
	if w.logFn != nil {
		return w.logFn(format, a...)
	}

	return 0, nil
}

func (w Worker) error(job Job, workerContext *Context, err error) {
	if w.jobErrorFn != nil {
		w.jobErrorFn(job, workerContext, err)
	}
}
