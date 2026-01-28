package gemq

import (
	"bytes"
	"encoding/json"
	"errors"
	"log"
	"sync"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"

	"github.com/glibtools/libs/util"
)

var (
	ErrTokenTimeout       = errors.New("mqtt token timeout")
	Inc                   = &Emq{}
	globalMqttTopicOption = &MqttTopicOption{
		Timeout: time.Second * 2,
		Retain:  false,
		Qos:     2,
	}
)

// Emq ...
type Emq struct {
	clientMux sync.RWMutex
	client    mqtt.Client
	prefix    string
}

// Client ...
func (e *Emq) Client() mqtt.Client {
	e.clientMux.RLock()
	defer e.clientMux.RUnlock()
	return e.client
}

// Connect ...
func (e *Emq) Connect(opt DialOptions) {
	logger := GetLogger()
	mqtt.ERROR = logger
	mqtt.CRITICAL = logger
	mqtt.WARN = logger
	e.prefix = opt.AppName + "/"
	log.Println("mqtt connecting...")
	c, err := DailWithOption(opt)
	if err != nil {
		log.Fatalf("mqtt connect error: %v\n", err)
		return
	}
	e.clientMux.Lock()
	e.client = c
	e.clientMux.Unlock()
	log.Println("mqtt connected")
}

// Publish ...
// args can be: *MqttTopicOption, MqttTopicOption, time.Duration, bool, byte
func (e *Emq) Publish(topic string, payload interface{}, args ...interface{}) (err error) {
	var buf bytes.Buffer
	switch v := payload.(type) {
	case []byte:
		buf.Write(v)
	case string:
		buf.WriteString(v)
	default:
		data, _ := json.Marshal(v)
		buf.Write(data)
	}
	topic = e.prefix + topic
	opt := getTopicOption(args...)
	t := e.client.Publish(topic, opt.Qos, opt.Retain, buf.Bytes())
	if !t.WaitTimeout(opt.Timeout) {
		return ErrTokenTimeout
	}
	return t.Error()
}

// Subscribe ...
// args can be: *MqttTopicOption, MqttTopicOption, time.Duration, bool, byte
func (e *Emq) Subscribe(topic string, callback mqtt.MessageHandler, args ...interface{}) (err error) {
	topic = e.prefix + topic
	opt := getTopicOption(args...)
	t := e.client.Subscribe(topic, opt.Qos, callback)
	if !t.WaitTimeout(opt.Timeout) {
		return ErrTokenTimeout
	}
	return t.Error()
}

type MqttTopicOption struct {
	Timeout time.Duration `json:"timeout,omitempty"`
	Retain  bool          `json:"retain,omitempty"`
	Qos     byte          `json:"qos,omitempty"`
}

func SetGlobalMqttTopicOption(opt *MqttTopicOption) { globalMqttTopicOption = opt }

func getTopicOption(args ...interface{}) (opt *MqttTopicOption) {
	opt = &MqttTopicOption{}
	util.MergeConfigs(opt, globalMqttTopicOption)
	for _, arg := range args {
		switch v := arg.(type) {
		case *MqttTopicOption:
			opt = v
		case MqttTopicOption:
			opt = &v
		case time.Duration:
			opt.Timeout = v
		case bool:
			opt.Retain = v
		case byte:
			opt.Qos = v
		}
	}
	return
}
