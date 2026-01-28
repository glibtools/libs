package util

import (
	"log"
	"runtime"
	"strconv"
	"sync"
)

var (
	// SingleInstance is a singleton instance
	localSingleInstance = &singleInstance{}
	_                   = log.Println
)

type singleInstance struct {
	m map[string]interface{}
	sync.RWMutex
}

// load the single instance
func (s *singleInstance) load(key string, newFunc func() interface{}) interface{} {
	s.Lock()
	defer s.Unlock()
	if s.m == nil {
		s.m = make(map[string]interface{})
	}
	if v, ok := s.m[key]; ok {
		return v
	}
	v := newFunc()
	s.m[key] = v
	return v
}

func LoadSingle[T any](newFunc func() T) T {
	//log.Printf("LoadSingle: %s", getRuntimePosition())
	return LoadSingleInstance[T](getRuntimePosition(), newFunc)
}

// LoadSingleInstance load the single instance
func LoadSingleInstance[T any](key string, newFunc func() T) T {
	return localSingleInstance.load(key, func() interface{} { return newFunc() }).(T)
}

func getRuntimePosition() string {
	_, file, line, ok := runtime.Caller(2)
	if !ok {
		return ""
	}
	return file + ":" + strconv.Itoa(line)
}
