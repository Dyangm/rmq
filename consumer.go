package rmq

import (
	log "github.com/sirupsen/logrus"
	"github.com/streadway/amqp"
	"time"
)

const ReconnectInterval = 5

type Consumer struct {
	uri       string
	queue     string
	tag       string
	conn      *amqp.Connection
	channel   *amqp.Channel
	MsgChan   chan *amqp.Delivery
	closeChan chan struct{}
	doneChan  chan error
}

func NewConsumer(uri, queue, tag string) (*Consumer, error) {
	c := &Consumer{}
	c.uri = uri
	c.queue = queue
	c.tag = tag
	c.closeChan = make(chan struct{})
	c.MsgChan = make(chan *amqp.Delivery)
	c.doneChan = make(chan error)
	msgChan, err := c.connect()

	if err != nil {
		return nil, err
	}

	go c.handle(msgChan)

	return c, nil
}

func (c *Consumer) Shutdown() error {
	close(c.closeChan)
	return <-c.doneChan
}

func (c *Consumer) connect() (<-chan amqp.Delivery, error) {
	conn, err := amqp.Dial(c.uri)
	if err != nil {
		return nil, err
	}

	ch, err := conn.Channel()
	if err != nil {
		return nil, err
	}
	ch.Qos(5000, 0, true)
	msgChan, err := ch.Consume(
		c.queue,
		c.tag, // consumer
		false, // auto-ack
		false, // exclusive
		false, // no-local
		false, // no-wait
		nil,   // args
	)
	if err != nil {
		return nil, err
	}

	c.conn = conn
	c.channel = ch

	return msgChan, nil
}

func (c *Consumer) reconnect() <-chan amqp.Delivery {
	for {
		log.Info("rmq reconneting")
		msgChan, err := c.connect()
		if err != nil {
			log.Errorf("rmq reconnect failed %s", err)
			time.Sleep(ReconnectInterval * time.Second)
		} else {
			log.Info("rmq reconnect success")
			return msgChan
		}

		select {
		case <-c.closeChan:
			return nil
		default:
			break
		}
	}
}

func (c *Consumer) handle(msgChan <-chan amqp.Delivery) {
	for {
		select {
		case msg, ok := <-msgChan:
			if !ok {
				c.conn.Close()
				msgChan = c.reconnect()
			} else {
				c.MsgChan <- &msg
				msg.Ack(false)
			}
		case <-c.closeChan:
			if err := c.channel.Cancel(c.tag, true); err != nil {
				c.doneChan <- err
				return
			}
			if err := c.conn.Close(); err != nil {
				c.doneChan <- err
				return
			}
			c.doneChan <- nil
			return
		}
	}

}
