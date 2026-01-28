package util

import (
	"io"
	"log"
)

type ItfLogger interface {
	Close() error
	SetOutput(writer io.Writer)
	Writer() io.Writer
	Print(i ...interface{})
	Printf(s string, i ...interface{})
	Println(i ...interface{})
	Fatal(i ...interface{})
	Fatalf(s string, i ...interface{})
	Fatalln(i ...interface{})
	Panic(i ...interface{})
	Panicf(s string, i ...interface{})
	Panicln(i ...interface{})
	Debugf(s string, i ...interface{})
	Infof(s string, i ...interface{})
	Warnf(s string, i ...interface{})
	Errorf(s string, i ...interface{})
	Debug(args ...interface{})
	Info(args ...interface{})
	Warn(args ...interface{})
	Error(args ...interface{})
}

type stdLoggerImpl struct {
	*log.Logger
}

func (l *stdLoggerImpl) Close() error {
	w, ok := l.Writer().(io.Closer)
	if ok {
		return w.Close()
	}
	return nil
}

func (l *stdLoggerImpl) Debug(args ...interface{}) {
	l.Logger.Println(args...)
}

func (l *stdLoggerImpl) Debugf(s string, i ...interface{}) {
	l.Logger.Printf(s, i...)
}

func (l *stdLoggerImpl) Error(args ...interface{}) {
	l.Logger.Println(args...)
}

func (l *stdLoggerImpl) Errorf(s string, i ...interface{}) {
	l.Logger.Printf(s, i...)
}

func (l *stdLoggerImpl) Info(args ...interface{}) {
	l.Logger.Println(args...)
}

func (l *stdLoggerImpl) Infof(s string, i ...interface{}) {
	l.Logger.Printf(s, i...)
}

func (l *stdLoggerImpl) Warn(args ...interface{}) {
	l.Logger.Println(args...)
}

func (l *stdLoggerImpl) Warnf(s string, i ...interface{}) {
	l.Logger.Printf(s, i...)
}
