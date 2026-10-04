package jwt

import (
	"net/http/httptest"
	"testing"
	"time"

	"github.com/kataras/iris/v12"
)

type matcherTestStore struct{ value *Token }

func (*matcherTestStore) ClearExpiredToken() {}
func (s *matcherTestStore) DelToken(string)  { s.value = nil }
func (s *matcherTestStore) GetToken(string) (*Token, bool) {
	if s.value == nil {
		return nil, false
	}
	v := *s.value
	return &v, true
}
func (s *matcherTestStore) SetToken(_ string, v *Token) { c := *v; s.value = &c }

func TestOptionalMatcherDefaultAndRefresh(t *testing.T) {
	s := &matcherTestStore{}
	j := &JWT{Expire: 1000, Store: s}
	bearer := j.AfterLogin("fixture-user").Token
	app := iris.New()
	if app.Build() != nil {
		t.Fatal("app build")
	}
	verify := func() bool {
		r := httptest.NewRequest("GET", "/", nil)
		r.Header.Set("Token", bearer)
		c := app.ContextPool.Acquire(httptest.NewRecorder(), r)
		defer app.ContextPool.Release(c)
		return j.Verify(c, func(string) (any, error) { return true, nil }) == nil
	}
	if s.value.Token != bearer || !verify() {
		t.Fatal("default store behavior changed")
	}
	s.value.ExpiresAt = time.Now().Unix() + 100
	if !verify() || s.value.ExpiresAt < time.Now().Unix()+990 || s.value.Token != bearer {
		t.Fatal("default refresh changed")
	}
	j.MatchToken = func(stored, presented string) bool { return stored == "opaque-verifier" && presented == bearer }
	s.value.Token = "opaque-verifier"
	if !verify() {
		t.Fatal("optional matcher not used")
	}
	j.Logout("fixture-user")
	if verify() {
		t.Fatal("logout changed")
	}
}
