package worker

type WorkFunction func(job Job) error
type LogFunction func(format string, a ...interface{}) (n int, err error)
