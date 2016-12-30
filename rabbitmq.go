// 该文件的实现存在与app自己的目录中
package memq

import (
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/streadway/amqp"
)

type rabbitmqPdrFactory struct {
	amqpUri, exchange, bindingKey, queue string
}

// 这里存储特定媒介需要的数据对象，一定要实现producer的send接口
type rabbitmqPdr struct {
	//
	conn *amqp.Connection

	//
	channel *amqp.Channel

	// 这个notify需要memq.go中进行处理，遇到pdr被关闭，需要从队列中去掉该pdr，重新补充一个
	notifyErr chan *amqp.Error

	// 用于保证mq消息可靠性，这里暂时统一做成都需要可靠发送的
	confirms chan amqp.Confirmation

	factory *rabbitmqPdrFactory
}

func NewRabbitmqPdrFactory(amqpUri, exchange, bindingKey, queue string) *rabbitmqPdrFactory {
	return &rabbitmqPdrFactory{
		amqpUri:    amqpUri,
		exchange:   exchange,
		bindingKey: bindingKey,
		queue:      queue,
	}
}

// TODO(lihao3) 这么设计的话，对redis或者rabbitmq的开发，需要memq自己封装，插件化会导致可能出现冲突，但不影响freeman的使用
func (factory *rabbitmqPdrFactory) GetName() string {
	return fmt.Sprintf("rabbitmq-%s-%s-%s", factory.exchange, factory.bindingKey, factory.queue)
}

// 初始化一个rabbitmq的channel对象
func (factory *rabbitmqPdrFactory) Create() (Producer, error) {
	s := rabbitmqPdr{
		notifyErr: make(chan *amqp.Error),
		confirms:  make(chan amqp.Confirmation, 1),

		factory: factory,
	}

	var (
		err error
	)

	s.conn, err = amqp.DialConfig(factory.amqpUri, amqp.Config{
		Heartbeat: 5 * time.Second,
	})
	if err != nil {
		return nil, err
	}
	s.notifyErr = s.conn.NotifyClose(make(chan *amqp.Error))

	s.channel, err = s.conn.Channel()
	if err != nil {
		return nil, fmt.Errorf("Getting Channel error: %s", err)
	}
	if err = s.channel.Confirm(false); err != nil {
		return nil, fmt.Errorf("confirm.select destination: %s", err)
	}
	s.channel.NotifyPublish(s.confirms)
	s.channel.Qos(500, 0, false)

	if err = s.channel.ExchangeDeclare(
		factory.exchange, // name of the exchange
		"direct",         // type 目前所有使用的均为direct，这里写死
		true,             // durable
		false,            // delete when complete
		false,            // internal
		false,            // noWait
		nil,              //arguments
	); err != nil {
		return nil, fmt.Errorf("AMQP exchange:%s declare error: %s", factory.exchange, err)
	}

	if _, err = s.channel.QueueDeclare(
		factory.queue, // name of the queue
		true,          // durable
		false,         // delete when used
		false,         // exclusive
		false,         // noWait
		nil,           // arguments
	); err != nil {
		return nil, fmt.Errorf("AMQP queue:%s declare error: %s", factory.queue, err)
	}

	if err = s.channel.QueueBind(
		factory.queue,      // name of the queue
		factory.bindingKey, // bindingKey
		factory.exchange,   // sourceExchange
		false,              // noWait
		nil,                // arguments
	); err != nil {
		return nil, fmt.Errorf("Queue bind (key %s) error: %s", factory.bindingKey, err)
	}

	return &s, nil
}

func (pdr *rabbitmqPdr) Send(msg interface{}) error {
	var rabbitMsg amqp.Publishing

	if ms, ok := msg.(string); ok {
		if err := json.Unmarshal([]byte(ms), &rabbitMsg); err != nil {
			return err
		}
	} else if bs, ok := msg.([]byte); ok {
		if err := json.Unmarshal(bs, &rabbitMsg); err != nil {
			return err
		}
	} else {
		rabbitMsg = msg.(amqp.Publishing)
	}

	if err := pdr.channel.Publish(pdr.factory.exchange, pdr.factory.bindingKey, false, false, rabbitMsg); err != nil {
		return err
	}

	select {
	case <-time.After(time.Second):
		return errors.New("publish confirm time exceed")
	case confirmed := <-pdr.confirms:
		if confirmed.Ack {
			return nil
		} else {
			return errors.New("publich confirm ack failed")
		}
	}

	return nil
}

func (pdr *rabbitmqPdr) Close() error {
	return pdr.conn.Close()
}
