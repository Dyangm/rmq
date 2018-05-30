package rmq

import "testing"

func TestNewConsumer(t *testing.T) {
	uri := "amqp://udc:123456@192.168.10.53:5672//udc_host"
	queue := "go_real_data_queue"
	c, err := NewConsumer(uri, queue, "rmqtest")
	if err != nil {
		t.Fail()
		return
	}

	defer c.Shutdown()

	msg := <-c.MsgChan
	if msg != nil {
		return
	}

	t.Fail()
}
