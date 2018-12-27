package worker

import "log"

type WorkFunction func(job Job) error
type JobErrorFunction func(job Job, err error)
type LogFunction func(format string, a ...interface{}) (n int, err error)

func NoLogFunction(format string, a ...interface{}) (n int, err error) {
	return 0, nil
}

func JobErrorsIgnoreFunction(job Job, err error) {

}

func JobErrorsFatalLogFunction(job Job, err error) {
	log.Fatal("job " + job.Name + " encounted fatal error: " + err.Error())
}