package work

import "time"

type Timer struct {
	NoOp       bool
	startTimes map[string]*time.Time
	totals     map[string]int64
	counts     map[string]int
}

type TimerRecord struct {
	Count     int
	TotalTime int64
	Label     string
}

func (t *Timer) Start(label string) {
	if t.NoOp {
		return
	}

	if t.startTimes == nil {
		t.startTimes = make(map[string]*time.Time)
	}

	if t.totals == nil {
		t.totals = make(map[string]int64)
	}

	if t.counts == nil {
		t.counts = make(map[string]int)
	}

	now := time.Now()
	t.startTimes[label] = &now
}

func (t *Timer) Stop(label string) {
	if t.NoOp {
		return
	}

	start := t.startTimes[label]
	if start != nil {
		timing := time.Now().Sub(*start).Nanoseconds()
		t.totals[label] += timing
		t.counts[label]++
	}
}

func (t *Timer) GetTimingsForLabel(label string) TimerRecord {
	return TimerRecord{
		Count:     t.counts[label],
		TotalTime: t.totals[label],
		Label:     label,
	}
}

func (t *Timer) GetTimings() []TimerRecord {
	var result []TimerRecord

	if !t.NoOp {
		for label, timing := range t.totals {
			count := t.counts[label]

			result = append(result, TimerRecord{
				Count:     count,
				TotalTime: timing,
				Label:     label,
			})
		}
	}

	return result
}
