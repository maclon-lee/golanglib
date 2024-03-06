package rabbitmq

import (
	"errors"
	"github.com/maclon-lee/golanglib/lib/config"
	"time"
)

/****
*rabbitmq 生产者封装
*rabbitmq统一使用 topic模式，如果需要 fanout，routingkey设置为#即可
*routingkey 尽量使用 1级类目.2级类目.3级类目方式
*******/

type Producer struct {
	client *client
}

const producerHeartbeat = time.Minute * 20

func NewProducer(connName string) (*Producer, error) {
	var p *Producer
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
	c := NewClient(addr, vHost, connName, producerHeartbeat)
	p = &Producer{
		client: c,
	}
	return p, nil
}

//发布消息
func (p Producer) Publish(message interface{}, exchangeName string, routingKey string) error {
	return p.client.Publish(message, exchangeName, routingKey)
}

//发布延时消息
//delay 毫秒
func (p Producer) PublishDelay(message interface{}, exchangeName string, routingKey string, delay int) error {
	return p.client.PublishDelay(message, exchangeName, routingKey, delay)
}

func (p *Producer) Close() error {
	if p.client != nil {
		return p.client.Close()
	}
	return nil
}
