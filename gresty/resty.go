package gresty

import (
	"crypto/tls"
	"errors"
	"fmt"
	"math/rand"
	"net"
	"net/http"
	"runtime"
	"sync"
	"time"

	"github.com/go-resty/resty/v2"
	"golang.org/x/net/proxy"
)

var RestyClient = new(Resty)

type Resty struct {
	Logger resty.Logger `inject:""`

	client *resty.Client
	once   sync.Once
}

// Client ...Client
func (r *Resty) Client() *resty.Client { return r.lazyInit().client }

func (r *Resty) Constructor() {
	r.lazyInit()
}

func (r *Resty) GetLogger() resty.Logger {
	if r.Logger == nil {
		r.Logger = &emptyLogger{}
	}
	return r.Logger
}

func (r *Resty) ResponseEnsureOK() *Resty {
	r.Client().OnAfterResponse(func(c *resty.Client, r *resty.Response) error {
		switch r.StatusCode() {
		case http.StatusOK:
			return nil
		default:
			return fmt.Errorf("%s\n[%s]\n%s\n", r.Request.URL, r.Status(), r.String())
		}
	})
	return r
}

func (r *Resty) SetLogger(Logger resty.Logger) {
	r.lazyInit()
	r.Logger = Logger
	r.client.SetLogger(Logger)
}

func (r *Resty) SetProxy(proxy string) *Resty {
	r.Client().SetProxy(proxy)
	return r
}

func (r *Resty) init() {
	c := resty.New()
	c.SetTransport(createTransport(nil))
	c.SetLogger(r.GetLogger())
	c.SetTimeout(time.Second * 20)
	c.SetRetryCount(3)
	c.SetRetryWaitTime(time.Millisecond * 300)
	c.SetRetryMaxWaitTime(time.Second * 2)
	c.SetAllowGetMethodPayload(true)
	//c.SetContentLength(true)
	c.SetHeaders(map[string]string{
		"Accept":        "*/*",
		"Pragma":        "no-cache",
		"Cache-Control": "no-cache",
		"Connection":    "keep-alive",
	})

	c.OnBeforeRequest(func(client *resty.Client, request *resty.Request) error {
		const ua = "User-Agent"
		if resty.IsStringEmpty(request.Header.Get(ua)) {
			request.SetHeader(ua, RandUseragent())
		}
		return nil
	})

	r.client = c
	//r.ResponseEnsureOK()
}

// lazyInit ......
func (r *Resty) lazyInit() *Resty {
	r.once.Do(r.init)
	return r
}

func AssignTransport(c *http.Client, fn func(t *http.Transport)) {
	t, err := HttpClientTransport(c)
	if err != nil {
		return
	}
	fn(t)
}

func Client() *resty.Client { return RestyClient.Client() }

func HttpClientTransport(c *http.Client) (*http.Transport, error) {
	if transport, ok := c.Transport.(*http.Transport); ok {
		return transport, nil
	}
	return nil, errors.New("current transport is not an *http.Transport instance")
}

func New() *Resty { return new(Resty).lazyInit() }

// RandUseragent ...
func RandUseragent() string {
	userAgents := []string{
		"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/104.0.0.0 Safari/537.36",
		"Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/535.11 (KHTML, like Gecko) Chrome/17.0.963.56 Safari/535.11",
		"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_7_3) AppleWebKit/535.20 (KHTML, like Gecko) Chrome/19.0.1036.7 Safari/535.20",
		"Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.1 (KHTML, like Gecko) Chrome/21.0.1180.71 Safari/537.1 LBBROWSER",
		"Mozilla/5.0 (Windows NT 6.1; Win64; x64; rv:2.0b13pre) Gecko/20110307 Firefox/4.0b13pre",
		"Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/61.0.3163.100 Safari/537.36",
		//"Mozilla/5.0 (X11; U; Linux i686; en-US; rv:1.8.0.12) Gecko/20070731 Ubuntu/dapper-security Firefox/1.5.0.12",
		//"Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 6.0; Acoo Browser; SLCC1; .NET CLR 2.0.50727; Media Center PC 5.0; .NET CLR 3.0.04506)",
		//"Mozilla/5.0 (X11; U; Linux i686; en-US; rv:1.9.0.8) Gecko Fedora/1.9.0.8-1.fc10 Kazehakase/0.5.6",
		//"Mozilla/5.0 (compatible; MSIE 9.0; Windows NT 6.1; Win64; x64; Trident/5.0; .NET CLR 3.5.30729; .NET CLR 3.0.30729; .NET CLR 2.0.50727; Media Center PC 6.0), Lynx/2.8.5rel.1 libwww-FM/2.14 SSL-MM/1.4.1 GNUTLS/1.2.9",
		//"Mozilla/4.0 (compatible; MSIE 6.0; Windows NT 5.1; SV1; .NET CLR 1.1.4322; .NET CLR 2.0.50727)",
		//"Mozilla/5.0 (compatible; MSIE 9.0; Windows NT 6.1; WOW64; Trident/5.0; SLCC2; .NET CLR 2.0.50727; .NET CLR 3.5.30729; .NET CLR 3.0.30729; Media Center PC 6.0; .NET4.0C; .NET4.0E; QQBrowser/7.0.3698.400)",
		//"Mozilla/4.0 (compatible; MSIE 6.0; Windows NT 5.1; SV1; QQDownload 732; .NET4.0C; .NET4.0E)",
		//"Opera/9.80 (Macintosh; Intel Mac OS X 10.6.8; U; fr) Presto/2.9.168 Version/11.52",
		//"Mozilla/5.0 (X11; U; Linux i686; en-US; rv:1.8.0.12) Gecko/20070731 Ubuntu/dapper-security Firefox/1.5.0.12",
		//"Mozilla/5.0 (compatible; MSIE 9.0; Windows NT 6.1; WOW64; Trident/5.0; SLCC2; .NET CLR 2.0.50727; .NET CLR 3.5.30729; .NET CLR 3.0.30729; Media Center PC 6.0; .NET4.0C; .NET4.0E; LBBROWSER)",
		//"Mozilla/5.0 (X11; U; Linux i686; en-US; rv:1.9.0.8) Gecko Fedora/1.9.0.8-1.fc10 Kazehakase/0.5.6",
		//"Mozilla/5.0 (X11; U; Linux; en-US) AppleWebKit/527+ (KHTML, like Gecko, Safari/419.3) Arora/0.6",
		//"Mozilla/5.0 (compatible; MSIE 9.0; Windows NT 6.1; WOW64; Trident/5.0; SLCC2; .NET CLR 2.0.50727; .NET CLR 3.5.30729; .NET CLR 3.0.30729; Media Center PC 6.0; .NET4.0C; .NET4.0E; QQBrowser/7.0.3698.400)",
		//"Opera/9.25 (Windows NT 5.1; U; en), Lynx/2.8.5rel.1 libwww-FM/2.14 SSL-MM/1.4.1 GNUTLS/1.2.9",
	}

	return userAgents[rand.Intn(len(userAgents))]
}

func SetTransport(c *http.Client, transport *http.Transport) {
	c.Transport = transport
}

// SetTransportDialer ...
func SetTransportDialer(c *http.Client, dialer proxy.ContextDialer) {
	transportVal, err := HttpClientTransport(c)
	if err != nil {
		return
	}
	transportVal.DialContext = dialer.DialContext
}

func createTransport(localAddr net.Addr) *http.Transport {
	dialer := &net.Dialer{
		Timeout:   10 * time.Second,
		KeepAlive: 30 * time.Second,
		//DualStack:     true,
		FallbackDelay: 500 * time.Millisecond,
	}
	if localAddr != nil {
		dialer.LocalAddr = localAddr
	}
	return &http.Transport{
		Proxy:                 http.ProxyFromEnvironment,
		DialContext:           dialer.DialContext,
		TLSClientConfig:       &tls.Config{InsecureSkipVerify: true},
		TLSHandshakeTimeout:   10 * time.Second,
		DisableKeepAlives:     false,
		DisableCompression:    false,
		MaxIdleConns:          20,
		MaxIdleConnsPerHost:   runtime.NumCPU() + 1,
		MaxConnsPerHost:       0,
		IdleConnTimeout:       29 * time.Second,
		ResponseHeaderTimeout: 0,
		ExpectContinueTimeout: 1 * time.Second,
		WriteBufferSize:       0,
		ReadBufferSize:        0,
		ForceAttemptHTTP2:     true,
	}
}
