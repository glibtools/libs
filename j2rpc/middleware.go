package j2rpc

import (
	"fmt"
	"log"
	"runtime"
)

func Recover() Handler {
	return func(c Context) {
		defer func() {
			if err := recover(); err != nil {
				c.Abort()
				stack := make([]byte, 4<<10)
				n := runtime.Stack(stack, false)
				stack = stack[:n]
				log.Printf("panic: %v\n%s", err, stack)
				c.WriteResponse(NewError(ErrInternal, fmt.Sprintf("panic: %v", err)))
			}
		}()
		c.Next()
	}
}
