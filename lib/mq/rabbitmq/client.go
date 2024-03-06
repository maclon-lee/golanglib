package rabbitmq

/****
*rabbitmq 客户端封装，调用时建议直接使用 consumer和 producer
*rabbitmq统一使用 topic模式，如果需要 fanout，routingkey设置为#即可
*routingkey 尽量使用 1级类目.2级类目.3级类目方式
*******/
import (
	"context"
	"errors"
	"fmt"
	"github.com/streadway/amqp"
	"github.com/maclon-lee/golanglib/lib/json"
	"strconv"
	"sync"
	"time"
)

const (
	reconnectDelay = 2 * time.Second // 连接断开后多久重连
	maxWaitNum     = 10              //最大等待次数
	exchangeType   = "topic"         //交换机模式使用topic，topic能满足所有的场景
	delayQueueName = "DelayQueueTemp"
)

type client struct {
	conn         *amqp.Connection
	channel      *amqp.Channel
	isConnected  bool
	connectMsg   chan bool
	done         chan struct{}
	notifyClose  chan *amqp.Error
	handlerFunc  func(amqp.Delivery) bool
	exchangeName string
	queueName    string
	routingKey   string

	connName  string
	addr      string
	vHost     string
	heartbeat time.Duration

	lock sync.Mutex
}

func NewClient(addr string, vHost string, connName string, heartbeat time.Duration) *client {
	c := &client{
		done:       make(chan struct{}),
		connectMsg: make(chan bool, 1),
	}
	c.addr = addr
	c.vHost = vHost
	c.connName = connName
	c.heartbeat = heartbeat

	go c.handleReconnect()
	return c
}

//处理断线重连
func (m *client) handleReconnect() {
	for {
		if !m.isConnected || m.conn.IsClosed() {
			m.lock.Lock()
			if !m.isConnected || m.conn.IsClosed() {
				if ok, _ := m.doConn(); !ok {
				}
			}
			m.lock.Unlock()
		}
		time.Sleep(reconnectDelay)
		select {
		case <-m.done:
			return
		case <-m.notifyClose:
			m.channel.Close()
			m.conn.Close()
			m.isConnected = false
		}
	}
}

//客户端连接操作
func (m *client) doConn() (bool, error) {
	conn, err := amqp.DialConfig(m.addr, amqp.Config{
		Heartbeat: m.heartbeat,
		Properties: amqp.Table{
			"connection_name": m.connName,
			"platform":        "golang",
		},
		Locale: "en_US",
		Vhost:  m.vHost,
	})
	if err != nil {
		return false, err
	}
	channel, err := conn.Channel()
	if err != nil {
		return false, err
	}

	m.notifyClose = make(chan *amqp.Error)
	channel.NotifyClose(m.notifyClose)

	m.conn = conn
	m.channel = channel
	m.isConnected = true
	m.connectMsg <- true
	if m.handlerFunc != nil {
		m.doConsume()
	}
	return true, nil
}

//消费逻辑
func (m *client) doConsume() {
	if ok, err := m.waitConn(); !ok {
		failOnError(err, "mq连接超时")
	}
	err := m.channel.ExchangeDeclare(
		m.exchangeName, // name of the exchange
		exchangeType,   // type
		true,           // durable
		false,          // delete when complete
		false,          // internal
		false,          // noWait
		nil,            // arguments
	)
	failOnError(err, "Failed to register an Exchange")
	queue, err := m.channel.QueueDeclare(
		m.queueName, // name of the queue
		true,        // durable
		false,       // delete when usused
		false,       // exclusive
		false,       // noWait
		nil,         // arguments
	)
	failOnError(err, "Failed to register an Queue")

	err = m.channel.QueueBind(
		queue.Name,     // name of the queue
		m.routingKey,   // bindingKey
		m.exchangeName, // sourceExchange
		false,          // noWait
		nil,            // arguments
	)
	failOnError(err, "Failed to register an Queue")
	_ = m.channel.Qos(1, 0, true)
	msgs, err := m.channel.Consume(
		queue.Name, // queue
		m.connName, // consumer
		false,      // auto-ack
		false,      // exclusive
		false,      // no-local
		false,      // no-wait
		nil,        // args
	)
	failOnError(err, "Failed to register a consumer")

	for {
		select {
		case <-m.done:
			return
		case msg, ok := <-msgs:
			if !ok {
				return
			}
			if !m.handlerFunc(msg) && !msg.Redelivered {
				_ = msg.Reject(true) //如果消费失败并且没有重回过队列，直接重回队列
			} else {
				_ = msg.Ack(false)
			}
		}
	}

}

//关闭连接
func (m *client) Close() error {
	err := m.channel.Close()
	err = m.conn.Close()
	close(m.done)
	m.isConnected = false
	m.connectMsg <- false
	close(m.connectMsg)
	return err
}

//waitConn 等待重连
func (m *client) waitConn() (isConn bool, err error) {
	if m.isConnected {
		return true, nil
	}
	cxt, cancel := context.WithTimeout(context.Background(), reconnectDelay*maxWaitNum)
	select {
	case <-cxt.Done():
		err = errors.New("Wait connect timeout.")
		return
	case isConn = <-m.connectMsg:
		cancel()
		if !isConn {
			err = errors.New("Client has closed.")
		}
		return
	}
}

func failOnError(err error, msg string) {
	if err != nil {
		panic(fmt.Sprintf("%s: %s", msg, err))
	}
}

//消费者订阅
func (m *client) Subscribe(exchangeName, queueName, routingKey string, handlerFunc func(amqp.Delivery) bool) error {
	m.exchangeName = exchangeName
	m.routingKey = routingKey
	m.queueName = queueName
	m.handlerFunc = handlerFunc
	if m.isConnected {
		m.doConsume()
	}
	return nil
}

//发布消息
func (m *client) Publish(message interface{}, exchangeName string, routingKey string) error {
	if ok, err := m.waitConn(); !ok {
		return err
	}
	if m.conn == nil {
		panic("No connection is initialized.")
	}

	err := m.channel.ExchangeDeclare(
		exchangeName, // name of the exchange
		exchangeType, // type
		true,         // durable
		false,        // delete when complete
		false,        // internal
		false,        // noWait
		nil,          // arguments
	)
	if err != nil {
		return err
	}
	body, _ := json.Marshal(message)
	err = m.channel.Publish( // Publishes a message onto the queue.
		exchangeName, // exchange
		routingKey,   // routing key      q.Name
		false,        // mandatory
		false,        // immediate
		amqp.Publishing{
			Body: body, // Our JSON body as []byte
		})
	return err
}

//发布延时消息,
//delay 毫秒
func (m *client) PublishDelay(message interface{}, exchangeName string, routingKey string, delay int) error {
	if ok, err := m.waitConn(); !ok {
		return err
	}
	if m.conn == nil {
		panic("No connection is initialized.")
	}

	err := m.channel.ExchangeDeclare(
		exchangeName, // name of the exchange
		exchangeType, // type
		true,         // durable
		false,        // delete when complete
		false,        // internal
		false,        // noWait
		nil,          // arguments
	)
	if err != nil {
		return err
	}

	queue, err := m.channel.QueueDeclare(
		delayQueueName, // name of the queue
		true,           // durable
		false,          // delete when usused
		false,          // exclusive
		false,          // noWait
		map[string]interface{}{
			"x-dead-letter-exchange":    exchangeName,
			"x-dead-letter-routing-key": routingKey,
		}, // arguments
	)
	if err != nil {
		return err
	}

	body, _ := json.Marshal(message)
	err = m.channel.Publish( // Publishes a message onto the queue.
		"",         // exchange
		queue.Name, // routing key      q.Name
		false,      // mandatory
		false,      // immediate
		amqp.Publishing{
			Body:       body, // Our JSON body as []byte
			Expiration: strconv.Itoa(delay),
		})
	return err
}
