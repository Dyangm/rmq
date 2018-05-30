package rmq

import (
	"testing"
)

func TestNewPublisher(t *testing.T) {
	uri := "amqp://udc:123456@192.168.10.53:5672//udc_host"
	routingKey := "realdata"
	exchange := "udc_exchange"
	c, err := NewPublisher(uri, exchange, routingKey, "rmqtest")
	if err != nil {
		t.Fail()
		return
	}

	defer c.Shutdown()

	msg := "hello world!"
	c.MsgChan <- []byte(msg)

	t.Fail()
}
