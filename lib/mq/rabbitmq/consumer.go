package rabbitmq

/****
*rabbitmq 消费者封装
*rabbitmq统一使用 topic模式，如果需要 fanout，routingkey设置为#即可
*routingkey 尽量使用 1级类目.2级类目.3级类目方式
*******/
import (
	"errors"
	"github.com/streadway/amqp"
	"github.com/maclon-lee/golanglib/lib/config"
	"time"
)

type Consumer struct {
	HandlerFunc  func([]byte) bool
	exchangeName string
	queueName    string
	routingKey   string
	client       *client
}

const consomerHeartbeat = time.Second * 10

func NewConsumer(exchangeName, queueName, routingKey, connName string, handlerFunc func([]byte) bool) (*Consumer, error) {
	var cs *Consumer
	var addr string
	var vHost string
	if config.IsSet("rabbitmq") {
		cfg := config.GetSubConfig("rabbitmq")
		addr = cfg.GetString("amqp")
		vHost = cfg.GetString("vHost")
	}
	if addr == "" {
		return nil, errors.New("rabbitmq配置不正确")
	}
	c := NewClient(addr, vHost, connName, consomerHeartbeat)
	cs = &Consumer{
		HandlerFunc:  handlerFunc,
		exchangeName: exchangeName,
		queueName:    queueName,
		routingKey:   routingKey,
		client:       c,
	}

	return cs, nil
}

func (c *Consumer) Subscribe() error {
	return c.client.Subscribe(c.exchangeName, c.queueName, c.routingKey, func(delivery amqp.Delivery) bool {
		return c.HandlerFunc(delivery.Body)
	})
}

func (c *Consumer) Close() error {
	if c.client != nil {
		return c.client.Close()
	}
	return nil
}
