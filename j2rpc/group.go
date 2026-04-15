package j2rpc

type rpcGroup struct {
	path     string
	handlers []Handler
}

func (g *rpcGroup) Handlers() []Handler { return g.handlers }

func (g *rpcGroup) Use(handlers ...Handler) Group {
	g.handlers = append(g.handlers, handlers...)
	return g
}

func NewGroup(path string) Group {
	return &rpcGroup{path: path, handlers: make([]Handler, 0)}
}
