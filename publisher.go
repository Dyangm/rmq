package rmq

import (
	"fmt"
	"github.com/streadway/amqp"
	"time"
)

type Publisher struct {
	uri       string
	exchange  string
	key       string
	tag       string
	MsgChan   chan []byte
	conn      *amqp.Connection
	channel   *amqp.Channel
	closeChan chan struct{}
	doneChan  chan error
	msgMap    [][]byte
	Err       error
}

func NewPublisher(uri, exchange, routingKey, tag string) (*Publisher, error) {
	p := &Publisher{}
	p.uri = uri
	p.exchange = exchange
	p.key = routingKey
	p.tag = tag

	p.MsgChan = make(chan []byte)
	p.closeChan = make(chan struct{})
	p.doneChan = make(chan error)
	p.msgMap = make([][]byte, 0)

	err := p.connect()
	if err != nil {
		return p, err
	}

	go p.handle()

	return p, nil
}

func (p *Publisher) Shutdown() error {
	close(p.closeChan)
	return <-p.doneChan
}

func (p *Publisher) connect() error {
	conn, err := amqp.Dial(p.uri)
	if err != nil {
		return err
	}

	ch, err := conn.Channel()
	if err != nil {
		return err
	}

	p.conn = conn
	p.channel = ch

	return nil
}

func (p *Publisher) sendMsg(msg []byte) error {
	err := p.channel.Publish(
		p.exchange, // exchange
		p.key,      // routing key
		false,      // mandatory
		false,
		amqp.Publishing{
			DeliveryMode: amqp.Persistent,
			ContentType:  "text/plain",
			Body:         msg,
		})
	return err
}

func (p *Publisher) reconnect() {
	for {
		fmt.Println("rmq reconneting")
		err := p.connect()
		if err != nil {
			fmt.Printf("rmq reconnect failed %s", err)
			time.Sleep(ReconnectInterval * time.Second)
		} else {
			fmt.Println("rmq reconnect success")
			return
		}

		select {
		case <-p.closeChan:
			return
		default:
			break
		}
	}
}

func (p *Publisher) handle() {
	for {
		select {
		case msg, ok := <-p.MsgChan:
			if ok {
				err := p.sendMsg(msg)
				if err != nil {
					p.Err = err
					p.reconnect()
					p.msgMap = append(p.msgMap, msg)
				} else {
					for i := 0; i < len(p.msgMap); i++ {
						p.sendMsg(p.msgMap[i])
					}
					p.msgMap = nil
				}
			} else {
				p.msgMap = append(p.msgMap, msg)
				p.reconnect()
			}
		case <-p.closeChan:
			if err := p.channel.Cancel(p.tag, true); err != nil {
				p.doneChan <- err
				return
			}
			if err := p.conn.Close(); err != nil {
				p.doneChan <- err
				return
			}
			p.doneChan <- nil
			return
		}
	}

}
