package util

import (
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/davecgh/go-spew/spew"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"gopkg.in/natefinch/lumberjack.v2"
)

var (
	_ = stdLogger
	_ = spew.Dump

	logWriterMapInstance = &logWriterMap{
		m: make(map[string]io.WriteCloser),
	}
)

type logWriterMap struct {
	sync.RWMutex
	m map[string]io.WriteCloser
}

// Close ...
func (l *logWriterMap) Close() {
	l.Lock()
	defer l.Unlock()
	for _, w := range l.m {
		_ = w.Close()
	}
}

// GetLogWriter ...
func (l *logWriterMap) GetLogWriter(name string) io.WriteCloser {
	l.RLock()
	w := l.m[name]
	l.RUnlock()
	if w != nil {
		return w
	}

	l.Lock()
	fileName := filepath.Join(RootDir(), "logs", fmt.Sprintf("%s.log", name))
	w = newLumberJackWriter(fileName)
	l.m[name] = w
	l.Unlock()
	return w
}

type zapSugarLogger struct {
	Sugar  *zap.SugaredLogger
	writer io.Writer
}

// Close ......
func (l *zapSugarLogger) Close() error {
	if c, ok := l.writer.(io.Closer); ok {
		return c.Close()
	}
	return nil
}

func (l *zapSugarLogger) Debug(args ...interface{}) {
	l.Sugar.Debug(args...)
}

func (l *zapSugarLogger) Debugf(s string, i ...interface{}) {
	l.Sugar.Debugf(s, i...)
}

func (l *zapSugarLogger) Error(args ...interface{}) {
	l.Sugar.Error(args...)
}

func (l *zapSugarLogger) Errorf(s string, i ...interface{}) {
	l.Sugar.Errorf(s, i...)
}

func (l *zapSugarLogger) Fatal(i ...interface{}) {
	l.Sugar.Fatal(i...)
}

func (l *zapSugarLogger) Fatalf(s string, i ...interface{}) {
	l.Sugar.Fatalf(s, i...)
}

func (l *zapSugarLogger) Fatalln(i ...interface{}) {
	l.Sugar.Fatalln(i...)
}

func (l *zapSugarLogger) Info(args ...interface{}) {
	l.Sugar.Info(args...)
}

func (l *zapSugarLogger) Infof(s string, i ...interface{}) {
	l.Sugar.Infof(s, i...)
}

func (l *zapSugarLogger) Panic(i ...interface{}) {
	l.Sugar.Panic(i...)
}

func (l *zapSugarLogger) Panicf(s string, i ...interface{}) {
	l.Sugar.Panicf(s, i...)
}

func (l *zapSugarLogger) Panicln(i ...interface{}) {
	l.Sugar.Panicln(i...)
}

func (l *zapSugarLogger) Print(i ...interface{}) {
	l.Sugar.Info(i...)
}

func (l *zapSugarLogger) Printf(s string, i ...interface{}) {
	l.Sugar.Infof(s, i...)
}

func (l *zapSugarLogger) Println(i ...interface{}) {
	l.Sugar.Infoln(i...)
}

func (l *zapSugarLogger) SetOutput(writer io.Writer) {
	l.writer = writer
}

func (l *zapSugarLogger) Warn(args ...interface{}) {
	l.Sugar.Warn(args...)
}

func (l *zapSugarLogger) Warnf(s string, i ...interface{}) {
	l.Sugar.Warnf(s, i...)
}

func (l *zapSugarLogger) Writer() io.Writer {
	return l.writer
}

func CloseWriters() {
	logWriterMapInstance.Close()
}

func GetLogWriter(logName string) io.WriteCloser {
	if logName == "" {
		return os.Stdout
	}
	return logWriterMapInstance.GetLogWriter(logName)
}

// ZapLogger ...获取一个logger实例
func ZapLogger(logName string, args ...string) ItfLogger {
	return zapLogger(logName, args...)
}

func getEncoder(prefixes ...string) zapcore.Encoder {
	var prefix = "sys"
	if len(prefixes) > 0 && prefixes[0] != "" {
		prefix = prefixes[0]
	}
	encoderConfig := zap.NewProductionEncoderConfig()
	encoderConfig.EncodeLevel = zapcore.CapitalLevelEncoder
	encoderConfig.EncodeTime = func(t time.Time, enc zapcore.PrimitiveArrayEncoder) {
		enc.AppendString(fmt.Sprintf("[%s] ", strings.ToUpper(prefix)) + t.Format("2006-01-02 15:04:05.000") + " ")
	}
	return zapcore.NewConsoleEncoder(encoderConfig)
}

func newLumberJackWriter(name string) io.WriteCloser {
	return &lumberjack.Logger{
		Filename:   name,
		MaxSize:    2,
		MaxBackups: 5,
		MaxAge:     3,
		Compress:   false,
		LocalTime:  true,
	}
}

func stdLogger(logName string, args ...string) ItfLogger {
	prefix := logName
	if len(args) > 1 && args[1] != "" {
		prefix = args[1]
	}
	writer := GetLogWriter(logName)
	lg := log.New(writer, prefix, log.LstdFlags|log.Lshortfile)
	return &stdLoggerImpl{Logger: lg}
}

func zapLogger(logName string, args ...string) ItfLogger {
	lvl := zapcore.DebugLevel
	if len(args) > 0 {
		lvl, _ = zapcore.ParseLevel(args[0])
	}
	prefix := logName
	if len(args) > 1 && args[1] != "" {
		prefix = args[1]
	}
	writer := GetLogWriter(logName)
	core := zapcore.NewCore(getEncoder(prefix), zapcore.AddSync(writer), lvl)
	return &zapSugarLogger{
		Sugar:  zap.New(core, zap.WithCaller(true), zap.AddCallerSkip(1)).Sugar(),
		writer: writer,
	}
}
