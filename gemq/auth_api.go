package gemq

import (
	"strings"
	"sync"

	"github.com/kataras/iris/v12"
	"github.com/kataras/iris/v12/middleware/accesslog"
	"github.com/kataras/iris/v12/middleware/cors"
	"github.com/kataras/iris/v12/middleware/recover"

	"github.com/glibtools/libs/giris"
)

var (
	API = &AuthAPI{}

	poolAuthRequest = &sync.Pool{
		New: func() interface{} {
			return &AuthRequest{}
		},
	}
)

type AuthAPI struct {
	AuthFunc    MqttFunc
	AclFunc     MqttFunc
	WriteLog    bool
	IsSuperFunc func(*AuthRequest) bool
	// StrictACL routes every ACL request to AclFunc (deny when unset): no
	// automatic allow for subscribe, pong or super names, no request access
	// log, and fixed error texts. Disabled keeps the historical behavior.
	StrictACL bool
}

// ACL ...
func (a *AuthAPI) ACL(c iris.Context, d *AuthRequest) (result *AuthResult, err error) {
	if a.StrictACL {
		if a.AclFunc == nil {
			return &AuthResult{Result: "deny"}, nil
		}
		return a.AclFunc(c, d)
	}
	if a.isSuper(d) || strings.HasSuffix(d.Topic, "/pong") || d.Action == "subscribe" {
		return &AuthResult{Result: "allow"}, nil
	}
	if a.AclFunc != nil {
		return a.AclFunc(c, d)
	}
	return &AuthResult{Result: "deny"}, nil
}

// Auth ...
func (a *AuthAPI) Auth(c iris.Context, d *AuthRequest) (result *AuthResult, err error) {
	if a.isSuper(d) {
		return &AuthResult{Result: "allow", IsSuperuser: true}, nil
	}
	if a.AuthFunc != nil {
		return a.AuthFunc(c, d)
	}
	return &AuthResult{Result: "deny"}, nil
}

// Handle ...
func (a *AuthAPI) Handle(r iris.Party) iris.Party {
	p := r.Party("/mqtt")
	p.UseRouter(recover.New(), cors.New().Handler())
	if a.WriteLog && !a.StrictACL {
		p.UseRouter(accesslog.New(GetLogger().Writer()).Handler)
	}
	p.Post("/auth", handlerMqtt(a.Auth, a.StrictACL))
	p.Post("/acl", handlerMqtt(a.ACL, a.StrictACL))
	return p
}

func (a *AuthAPI) isSuper(d *AuthRequest) bool {
	if a.IsSuperFunc != nil {
		return a.IsSuperFunc(d)
	}
	return d.IsSuperuser()
}

type AuthRequest struct {
	Username   string `json:"username,omitempty"`
	Password   string `json:"password,omitempty"`
	ClientId   string `json:"clientid,omitempty"`
	PeerHost   string `json:"peerhost,omitempty"`
	ProtoName  string `json:"proto_name,omitempty"`
	MountPoint string `json:"mountpoint,omitempty"`
	Action     string `json:"action,omitempty"`
	Topic      string `json:"topic,omitempty"`
	Qos        string `json:"qos,omitempty"`
	Retain     string `json:"retain,omitempty"`
}

func (a *AuthRequest) IsSuperuser() bool {
	peerHostPrefixList := []string{"192", "10", "172", "127", "::1", "localhost", "fe80", "fd00"}
	for _, prefix := range peerHostPrefixList {
		if strings.HasPrefix(a.PeerHost, prefix) && strings.HasPrefix(a.Username, "sys_") {
			return true
		}
	}
	return false
}

// Reset Empty struct to be used in PoolAuthRequest
func (a *AuthRequest) Reset() {
	a.Username = ""
	a.Password = ""
	a.ClientId = ""
	a.PeerHost = ""
	a.ProtoName = ""
	a.MountPoint = ""
	a.Action = ""
	a.Topic = ""
	a.Qos = ""
	a.Retain = ""
}

type AuthResult struct {
	Result      string `json:"result"`
	IsSuperuser bool   `json:"is_superuser,omitempty"`
}

type MqttFunc func(c iris.Context, d *AuthRequest) (result *AuthResult, err error)

// handlerMqtt resets pooled requests on borrow and return, so a body that
// omits a field never inherits it from an earlier request. In strict mode
// errors are reported with fixed texts only.
func handlerMqtt(fn MqttFunc, strict bool) iris.Handler {
	return func(c iris.Context) {
		d := poolAuthRequest.Get().(*AuthRequest)
		d.Reset()
		defer func() {
			d.Reset()
			poolAuthRequest.Put(d)
		}()
		if err := c.ReadJSON(d); err != nil {
			msg := "invalid request"
			if !strict {
				msg = err.Error()
			}
			giris.RestJSONError(c, 400, msg, true)
			return
		}
		r, err := fn(c, d)
		if err != nil {
			msg := "request failed"
			if !strict {
				msg = err.Error()
			}
			giris.RestJSONError(c, 500, msg, true)
			return
		}
		_ = c.JSON(r)
	}
}
