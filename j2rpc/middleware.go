package j2rpc

import (
	"fmt"
	"log"
	"runtime"
)

type StdLogger interface {
	Printf(format string, v ...interface{})
	Print(v ...interface{})
	Println(v ...interface{})
}

func Recover(args ...any) Handler {
	var logger StdLogger
	for _, arg := range args {
		switch _arg := arg.(type) {
		case StdLogger:
			logger = _arg
		}
	}
	if logger == nil {
		logger = log.Default() // use the default logger if no custom logger is provided
	}

	return func(c Context) {
		defer func() {
			if err := recover(); err != nil {
				c.Abort()
				stack := make([]byte, 4<<10)
				n := runtime.Stack(stack, false)
				stack = stack[:n]
				logger.Printf("panic: %v\n%s", err, stack)
				c.WriteResponse(NewError(ErrInternal, fmt.Sprintf("panic: %v", err)))
			}
		}()
		c.Next()
	}
}
