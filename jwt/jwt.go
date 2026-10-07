package jwt

import (
	"fmt"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/kataras/iris/v12"

	"github.com/glibtools/libs/crypto"
	"github.com/glibtools/libs/j2rpc"
	"github.com/glibtools/libs/util"
)

const (
	ErrorTokenMissing     = "禁止访问"
	ErrorTokenMalformed   = "令牌错误"
	ErrorTokenNotValidYet = "令牌不存在"
	ErrorTokenExpired     = "令牌过期"
	ErrorTokenInvalid     = "令牌无效"
)
const (
	TokenKey       = "__JWT_TOKEN"
	ContextUserKey = "__CONTEXT_USER"
)

var (
	innerJwtCryptoKey = []byte("FUXqWKtBONoR85Eb")[:16]

	PrefixJWTKeyFunc = func() string {
		return fmt.Sprintf("%s-jwt", util.AppName)
	}

	GetJwtKeyFunc = func(id string) string {
		return fmt.Sprintf("%s:%s", PrefixJWTKeyFunc(), id)
	}
)

// JWT is middleware for the RPC web framework that provides JWT authentication.
type JWT struct {
	Expire int64
	Store  ItfTokenStore
	// MatchToken optionally compares a stored verifier to the presented bearer.
	// Nil preserves the historical plaintext-store behavior.
	MatchToken func(stored, presented string) bool
	once       sync.Once
}

// AfterLogin ...
func (j *JWT) AfterLogin(id string) (t *Token) {
	t = &Token{ID: id}
	t.ExpiresAt = util.TimeNow().Unix() + j.Expire
	t.Token = generateToken(id)
	j.Store.SetToken(getJWTKey(id), t)
	return
}

// Logout ...
func (j *JWT) Logout(id string) { j.Store.DelToken(getJWTKey(id)) }

// New ...
func (j *JWT) New() *JWT { return j.lazyInit() }

func (*JWT) User(ctx iris.Context) interface{} { return ctx.Values().Get(ContextUserKey) }

// Verify verifies the token and returns the user ID if the token is valid.
func (j *JWT) Verify(c iris.Context, call func(id string) (user interface{}, err error)) (err error) {
	token := c.URLParam("token")
	if token == "" {
		token = c.GetHeader("token")
	}
	if token == "" {
		token = c.GetHeader("Authorization")
	}

	token = strings.ReplaceAll(token, "Bearer ", "")
	if token == "" {
		err = j2rpc.NewError(j2rpc.ErrAuthorization, ErrorTokenMissing)
		return
	}
	c.Values().Set(TokenKey, token)
	id := getIDFromToken(token)
	if id == "" {
		err = j2rpc.NewError(j2rpc.ErrAuthorization, ErrorTokenMalformed)
		return
	}
	t, has, err := j.findToken(getJWTKey(id), token)
	if err != nil {
		return
	}
	if !has {
		err = j2rpc.NewError(j2rpc.ErrAuthorization, ErrorTokenNotValidYet)
		return
	}
	if t.expired() {
		err = j2rpc.NewError(j2rpc.ErrAuthorization, ErrorTokenExpired)
		return
	}
	matches := t.Token == token
	if j.MatchToken != nil {
		matches = j.MatchToken(t.Token, token)
	}
	if !matches {
		err = j2rpc.NewError(j2rpc.ErrAuthorization, ErrorTokenInvalid)
		return
	}
	user, err := call(t.ID)
	if err != nil {
		return
	}
	j.refreshToken(t)
	c.Values().Set(ContextUserKey, user)
	return
}

// findToken uses the optional store lookup when available, else the historical
// single-key lookup. A finder error (store unavailable) is returned as is.
func (j *JWT) findToken(key, presented string) (*Token, bool, error) {
	if f, ok := j.Store.(ItfTokenFinder); ok {
		return f.FindToken(key, presented)
	}
	t, has := j.Store.GetToken(key)
	return t, has, nil
}

// lazyInit ......
func (j *JWT) lazyInit() *JWT {
	j.once.Do(func() {
		if j.Expire == 0 {
			// * days
			j.Expire = 3600 * 24 * 7
		}
		if j.Store == nil {
			j.Store = newDefaultTokenStore()
		}
		if j.Store != nil {
			j.runClearExpiredToken()
		}
	})
	return j
}

// refreshToken ...
func (j *JWT) refreshToken(t *Token) {
	if t.ExpiresAt-util.TimeNow().Unix() > j.Expire/2 {
		return
	}
	t.ExpiresAt = util.TimeNow().Unix() + j.Expire
	key := t.StorageKey
	if key == "" {
		key = getJWTKey(t.ID)
	}
	j.Store.SetToken(key, t)
}

func (j *JWT) runClearExpiredToken() { go util.Ticker(time.Hour*10, j.Store.ClearExpiredToken) }

type Token struct {
	ID        string `json:"id,omitempty"`
	ExpiresAt int64  `json:"expires_at,omitempty"`
	Token     string `json:"token,omitempty"`
	// StorageKey is the store record selected by ItfTokenFinder; never serialized.
	StorageKey string `json:"-"`
}

func (t *Token) expired() bool { return t.ExpiresAt < util.TimeNow().Unix() }

func DefaultJWT() *JWT { return new(JWT).New() }

func NewJWTWithStore(store ItfTokenStore) *JWT { return (&JWT{Store: store}).New() }

func SetGlobalCryptoKey(key string) {
	if !slices.Contains([]int{16, 24, 32}, len(key)) {
		panic("jwt crypto key length must be 16, 24 or 32")
	}
	innerJwtCryptoKey = []byte(key)
}

// generateToken ...
func generateToken(id string) string {
	v, _ := crypto.AESCBCEncrypt(innerJwtCryptoKey, []byte(id), true)
	return string(v)
}

func getIDFromToken(token string) string {
	v, _ := crypto.AESCBCDecrypt(innerJwtCryptoKey, []byte(token), true)
	return string(v)
}

func getJWTKey(id string) string {
	return GetJwtKeyFunc(id)
}
