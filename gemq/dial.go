package gemq

import (
	"errors"
	"fmt"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"

	"github.com/glibtools/libs/util"
)

type DialOptions struct {
	AppName string
	//tcp://host:port
	Broker   string
	Username string
	Password string
	// on connect handler
	OnConnectHandler mqtt.OnConnectHandler
}

func DailWithOption(opt DialOptions) (c mqtt.Client, err error) {
	o1 := mqtt.NewClientOptions()
	o1.AddBroker(opt.Broker)
	o1.SetOrderMatters(false)
	o1.SetStore(mqtt.NewMemoryStore())
	o1.SetConnectRetry(true)
	o1.SetAutoReconnect(true)
	o1.SetMaxReconnectInterval(time.Second * 10)
	o1.SetKeepAlive(60 * time.Second)
	o1.SetPingTimeout(2 * time.Second)
	o1.SetConnectTimeout(3 * time.Second)
	appName := opt.AppName
	o1.SetClientID(fmt.Sprintf("%s:%s", appName, util.UUID16md5hex()))
	username, password := opt.Username, opt.Password
	o1.SetUsername(fmt.Sprintf("%s:sys_%s", appName, username))
	o1.SetPassword(password)
	o1.SetOnConnectHandler(opt.OnConnectHandler)
	client := mqtt.NewClient(o1)
	tk := client.Connect()
	if !tk.WaitTimeout(3 * time.Second) {
		err = errors.New("mqtt connect timeout")
		return
	}
	if err = tk.Error(); err != nil {
		return
	}
	c = client
	return
}

func GetLogger() util.ItfLogger {
	return util.ZapLogger("mqtt", "debug", "mqtt")
}
