package work

import (
	"sync"
	"time"
)

type Timer struct {
	NoOp       bool
	startTimes map[string]*time.Time
	totals     map[string]int64
	counts     map[string]int
	mapMutex   *sync.Mutex
}

func NewTimer() *Timer {
	return &Timer{
		NoOp: false,
		startTimes: make(map[string]*time.Time),
		totals: make(map[string]int64),
		counts: make(map[string]int),
		mapMutex: &sync.Mutex{},
	}
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

	t.mapMutex.Lock()
	now := time.Now()
	t.startTimes[label] = &now
	t.mapMutex.Unlock()
}

func (t *Timer) Stop(label string) {
	if t.NoOp {
		return
	}

	t.mapMutex.Lock()
	start := t.startTimes[label]
	if start != nil {
		timing := time.Now().Sub(*start).Nanoseconds()
		t.totals[label] += timing
		t.counts[label]++
	}
	t.mapMutex.Unlock()
}

func (t *Timer) GetTimingsForLabel(label string) TimerRecord {
	t.mapMutex.Lock()
	result := TimerRecord{
		Count:     t.counts[label],
		TotalTime: t.totals[label],
		Label:     label,
	}
	t.mapMutex.Unlock()
	return result
}

func (t *Timer) GetTimings() []TimerRecord {
	var result []TimerRecord

	if !t.NoOp {
		t.mapMutex.Lock()
		for label, timing := range t.totals {
			count := t.counts[label]

			result = append(result, TimerRecord{
				Count:     count,
				TotalTime: timing,
				Label:     label,
			})
		}
		t.mapMutex.Unlock()
	}

	return result
}
