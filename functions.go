package worker

type WorkFunction func(job Job) error
type LogFunction func(format string, a ...interface{}) (n int, err error)

func NoLogFunction(format string, a ...interface{}) (n int, err error) {
	return 0, nil
}