package queue

import (
	"log"

	"github.com/glibtools/libs/util"
)

type GQHandlerMap map[string]util.GQueueHandler

func EnqueueGq(task string, payload interface{}, opt ...util.QueueTaskOpt) (err error) {
	var data []byte
	switch p := payload.(type) {
	case []byte:
		data = p
	case string:
		data = []byte(p)
	default:
		data, err = util.Marshal(payload)
		if err != nil {
			return
		}
	}
	GetGQServer().Enqueue(util.NewQueueTask(task, data, opt...))
	return
}

func GetGQServer() (server *util.GQueue) {
	return util.LoadSingle(func() *util.GQueue {
		return util.NewGQueue(util.GQueueOption{
			MaxWorkers: 10,
			MaxRetry:   3,
		})
	})
}

func StartGQServer(hMap GQHandlerMap) {
	server := GetGQServer()
	// register task handler
	for task, handler := range hMap {
		log.Printf("register task handler: %s\n", task)
		server.Register(task, handler)
	}
	server.StartServer()
	log.Println("GQServer started")
}
