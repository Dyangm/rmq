package rmq

import (
	"testing"
)

func TestNewPublisher(t *testing.T) {
	uri := "amqp://udc:123456@192.168.10.53:5672//udc_host"
	routingKey := "realdata"
	exchange := "test_exchange"
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

func BenchmarkPublisher(b *testing.B) {
	uri := "amqp://udc:123456@192.168.10.53:5672//udc_host"
	routingKey := "datahub_upload_real"
	exchange := "test_exchange"
	p, err := NewPublisher(uri, exchange, routingKey, "rmqtest")
	defer p.Shutdown()
	if err != nil {
		return
	}

	msg := "hello world"
	for i := 0; i < b.N; i++ {
		p.MsgChan <- []byte(msg)
	}
}
