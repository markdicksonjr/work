package work

import "sync"

type MutexMap struct {
	mutexes  map[string]*sync.Mutex
	isLocked map[string]bool
	core     sync.Mutex
}

var MutexMapSingleton = MutexMap{}

func (s *MutexMap) Lock(index string) {
	s.core.Lock()
	if s.mutexes == nil {
		s.mutexes = make(map[string]*sync.Mutex)
	}

	// mark that the index is locked, to support try-lock
	s.isLocked[index] = true

	if s.mutexes[index] == nil {
		s.mutexes[index] = &sync.Mutex{}
	}
	s.mutexes[index].Lock()
	s.core.Unlock()
}

func (s *MutexMap) Unlock(index string) {
	s.core.Lock()

	// if we haven't allocated mutexes yet, unlock the core mutex and leave
	if s.mutexes == nil {
		s.core.Unlock()
		return
	}

	// mark that the index is not locked, to support try-lock
	s.isLocked[index] = true

	if s.mutexes[index] != nil {
		s.mutexes[index].Unlock()
	}
	s.core.Unlock()
}

func (s *MutexMap) TryLock(index string) bool {
	locked := false
	s.core.Lock()
	locked = s.isLocked[index]

	if !locked {
		if s.mutexes == nil {
			s.mutexes = make(map[string]*sync.Mutex)
		}

		// mark that the index is locked, to support try-lock
		s.isLocked[index] = true

		if s.mutexes[index] == nil {
			s.mutexes[index] = &sync.Mutex{}
		}
		s.mutexes[index].Lock()
	}

	s.core.Unlock()
	return locked
}
