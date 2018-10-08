package worker

import (
	"sync/atomic"
)

type Job struct {
	Name			string
	Context			interface{}
	IsEndOfStream	bool
}

func NewWorker(id int, workerPool chan chan Job, workFn WorkFunction, logFn LogFunction) Worker {
	return Worker{
		id:         id,
		jobQueue:   make(chan Job),
		workerPool: workerPool,
		quitChan:   make(chan bool),
		workFn:		workFn,
		logFn:		logFn,
	}
}

type Worker struct {
	id				int
	jobQueue		chan Job
	workerPool		chan chan Job
	quitChan		chan bool
	workFn			WorkFunction
	runningCount	int32
	logFn			LogFunction
}

func (w Worker) GetRunningCount() int32 {
	return w.runningCount
}

func (w *Worker) start() {
	go func() {
		for {
			w.workerPool <- w.jobQueue

			select {
			case job := <-w.jobQueue:
				atomic.AddInt32(&w.runningCount, 1)
				w.logFn("worker%d: started %s\n", w.id, job.Name)
				err := w.workFn(job)
				atomic.AddInt32(&w.runningCount, -1)

				// TODO: improve error handling
				if err != nil {
					w.logFn("worker%d: had error in %s: %s!\n", w.id, job.Name, err.Error())
				}

				w.logFn("worker%d: completed %s!\n", w.id, job.Name)
			case <-w.quitChan:
				w.logFn("worker%d stopping\n", w.id)
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
