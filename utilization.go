package work

type Utilization struct {
	ByWorker           []WorkerUtilization
	PercentUtilization float32
}

type WorkerUtilization struct {
	PercentUtilization float32
	Id                 int
}

