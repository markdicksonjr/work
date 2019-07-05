package work

import "log"

type WorkFunction func(job Job, workerContext *Context) error
type JobErrorFunction func(job Job, workerContext *Context, err error)
type LogFunction func(format string, a ...interface{}) (n int, err error)

func NoLogFunction(format string, a ...interface{}) (n int, err error) {
	return 0, nil
}

func JobErrorsIgnoreFunction(job Job, err error) {

}

func JobErrorsFatalLogFunction(job Job, workerContext *Context, err error) {
	log.Fatal("job " + job.Name + " encounted fatal error: " + err.Error())
}

// MutexFunction is a function that will only ever be run at most once at any given time
type MutexFunction struct {
	dispatcher *Dispatcher
}

func NewMutexFunction(
	handler func(data interface{}) error,
	errFn func(interface{}, error),
) *MutexFunction {
	s := MutexFunction{
		dispatcher: NewDispatcher(1, 1, func(job Job, workerContext *Context) error {
			return handler(job.Context)
		}).WithJobErrFn(func(job Job, workerContext *Context, err error) {
			errFn(job.Context, err)
		}),
	}
	return &s
}

func (m *MutexFunction) Call(data interface{}) error {
	m.dispatcher.EnqueueJobAllowWait(Job{
		Context: data,
	})
	return nil
}

func (m *MutexFunction) WaitUntilIdle() {
	m.dispatcher.WaitUntilIdle()
}
