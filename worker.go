package worker

import (
	"fmt"
	"sync/atomic"
)

type Job struct {
	Name	string
	Context	interface{}
}

func NewWorker(id int, workerPool chan chan Job, workFn func(job Job) error) Worker {
	return Worker{
		id:         id,
		jobQueue:   make(chan Job),
		workerPool: workerPool,
		quitChan:   make(chan bool),
		workFn:		workFn,
	}
}

type Worker struct {
	id				int
	jobQueue		chan Job
	workerPool		chan chan Job
	quitChan		chan bool
	workFn			func(job Job) error
	runningCount	int32
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
				fmt.Printf("worker%d: started %s\n", w.id, job.Name)
				err := w.workFn(job)
				atomic.AddInt32(&w.runningCount, -1)

				// TODO: improve error handling
				if err != nil {
					fmt.Printf("worker%d: had error in %s: %s!\n", w.id, job.Name, err.Error())
				}

				fmt.Printf("worker%d: completed %s!\n", w.id, job.Name)
			case <-w.quitChan:
				fmt.Printf("worker%d stopping\n", w.id)
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
