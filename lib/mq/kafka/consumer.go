package kafka

import (
	"context"
	"errors"
	"github.com/Shopify/sarama"
	"github.com/maclon-lee/golanglib/lib/config"
	logger "github.com/maclon-lee/golanglib/lib/log"
	"os"
	"os/signal"
	"time"
)

var groupers map[string]sarama.ConsumerGroup
var groupAddress []string
var groupConfig *sarama.Config
var groupHandlers map[string]sarama.ConsumerGroupHandler
var groupConsumers map[string]*batchConsumer
var groupCluster sarama.ClusterAdmin
var bufferLength = 8

//类型：获取消息回调函数
type ConsumerCallback func([]*sarama.ConsumerMessage) bool

type batchConsumer struct {
	BatchSize      int32
	BatchInterval  time.Duration
	CallBack       ConsumerCallback
	MessageBuffer  []*sarama.ConsumerMessage
	callBackBuffer chan []*sarama.ConsumerMessage
	Session        sarama.ConsumerGroupSession
	done           chan struct{}
	currentIndex   int32
}
func newBatchConsumer(batchSize int32, batchInterval time.Duration, callback ConsumerCallback) *batchConsumer {
	if batchSize < 1 { //小于1的批量没有意义
		batchSize = 1
	}
	c := &batchConsumer{
		BatchSize:      batchSize,
		BatchInterval:  batchInterval,
		CallBack:       callback,
		callBackBuffer: make(chan []*sarama.ConsumerMessage, bufferLength),
		done:           make(chan struct{}),
	}
	go c.run()
	return c
}

func (c *batchConsumer) run() {
	for {
		select {
		case <-c.done:
			return
		case msgs := <-c.callBackBuffer:
			if c.CallBack(msgs) {
				m := msgs[len(msgs)-1]
				//根据消息标记消费模式
				//sess.MarkMessage(m, "")
				//根据偏移量标记消费模式
				c.Session.MarkOffset(m.Topic, m.Partition, m.Offset+1, "")
				//立即提交偏移量
				c.Session.Commit()
			}

		}
	}
}

func (c *batchConsumer) Close() {
	close(c.done)
}

func (c *batchConsumer) attach(msg *sarama.ConsumerMessage, sess sarama.ConsumerGroupSession, left int) {
	if c.CallBack == nil {
		return
	}
	c.Session = sess
	if c.MessageBuffer == nil {
		c.MessageBuffer = make([]*sarama.ConsumerMessage, c.BatchSize)
	}
	c.MessageBuffer[c.currentIndex] = msg
	c.currentIndex++
	if c.currentIndex == c.BatchSize || left == 0 { //buffer已满 或者队列里面已经没有数据 ，就完成一个批次
		c.sendToCallback()
	}
}

func (c *batchConsumer) sendToCallback() {
	batchMessage := make([]*sarama.ConsumerMessage, c.currentIndex)
	copy(batchMessage, c.MessageBuffer)
	c.callBackBuffer <- batchMessage
	c.MessageBuffer = nil
	c.currentIndex = 0
}

type kafkaConsumerGroupHandler struct {
	GroupID string
}

func (kafkaConsumerGroupHandler) Setup(_ sarama.ConsumerGroupSession) error {
	return nil
}
func (kafkaConsumerGroupHandler) Cleanup(_ sarama.ConsumerGroupSession) error {
	return nil
}
func (h kafkaConsumerGroupHandler) ConsumeClaim(sess sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for msg := range claim.Messages() {
		left := len(claim.Messages())
		//logger.Debugf("message left %d",left)
		if c, ok := groupConsumers[h.GroupID+msg.Topic]; ok {
			c.attach(msg, sess, left)
		}
	}
	return nil
}

/*
* 接收消费消息
*
* param  topic          主题
* param  groupId        消费组名称
* param  batchSize      批量消费数量
* param  batchInterval  批量消费间隔时间
* param  callback       获取消息回调函数
 */
func ReceiveMessage(topic, groupId string, batchSize int32, batchInterval time.Duration, callback ConsumerCallback) error {
	var err error
	var ok bool

	//初始化
	if groupers == nil {
		if groupAddress == nil || len(groupAddress) == 0 {
			cfglist := config.GetSubConfig("kafka")
			if cfglist != nil {
				groupAddress = cfglist.GetStringSlice("address")
				if len(groupAddress) == 0 {
					return errors.New("kafka address not config")
				}
			} else {
				return errors.New("kafka not config")
			}
		}

		if groupConfig == nil {
			groupConfig = sarama.NewConfig()
			groupConfig.Version = sarama.V1_0_0_0
			groupConfig.Consumer.Return.Errors = true
			groupConfig.Consumer.Fetch.Max = batchSize
			groupConfig.Consumer.Offsets.AutoCommit.Enable = false
			groupConfig.Consumer.Offsets.Initial = sarama.OffsetOldest
		}

		if groupHandlers == nil {
			groupHandlers = make(map[string]sarama.ConsumerGroupHandler)
		}

		if groupConsumers == nil {
			groupConsumers = make(map[string]*batchConsumer)
		}

		groupers = make(map[string]sarama.ConsumerGroup)

		signals := make(chan os.Signal, 1)
		signal.Notify(signals, os.Interrupt)

		go func() {
			for {
				select {
				case <-signals:
					closeReceive()
					return
				}
			}
		}()
	}

	var grouper sarama.ConsumerGroup
	if grouper, ok = groupers[groupId]; !ok || grouper == nil {
		grouper, err = sarama.NewConsumerGroup(groupAddress, groupId, groupConfig)
		if err != nil {
			return err
		}

		// Track errors
		go func(gr sarama.ConsumerGroup) {
			for err := range gr.Errors() {
				logger.Warnf("ConsumerGroup ERROR:%s", err)
			}
		}(grouper)

		groupers[groupId] = grouper
		groupHandlers[groupId] = kafkaConsumerGroupHandler{
			GroupID: groupId,
		}
	}

	// Iterate over consumer sessions.
	ctx := context.Background()
	topics := []string{topic}

	if _, ok := groupConsumers[groupId+topic]; !ok {
		groupConsumers[groupId+topic] = newBatchConsumer(batchSize, batchInterval, callback)
	}

	// `Consume` should be called inside an infinite loop, when a
	// server-side rebalance happens, the consumer session will need to be
	// recreated to get the new claims
	err = grouper.Consume(ctx, topics, groupHandlers[groupId])
	if err != nil {
		return err
	}

	return nil
}

/*
* 关闭消费消息
 */
func closeReceive() {
	if groupConsumers != nil {
		for _, c := range groupConsumers {
			c.Close()
		}
	}
	if groupers != nil {
		for _, grouper := range groupers {
			grouper.Close()
		}
	}
}

/*
* 获取kafka消费组最大可用偏移量
 */
func GetMessageLastOffset(topic, groupId string, partition int32) (int64, error) {
	if groupAddress == nil || len(groupAddress) == 0 {
		cfglist := config.GetSubConfig("kafka")
		if cfglist != nil {
			groupAddress = cfglist.GetStringSlice("address")
			if len(groupAddress) == 0 {
				return -1, errors.New("kafka address not config")
			}
		} else {
			return -1, errors.New("kafka not config")
		}
	}

	var err error
	if groupCluster == nil {
		groupCluster, err = sarama.NewClusterAdmin(groupAddress, nil)
		if err != nil {
			return -1, err
		}
	}

	topicPartitions := make(map[string][]int32)
	topicPartitions[topic] = []int32{partition}
	offResp, err := groupCluster.ListConsumerGroupOffsets(groupId, topicPartitions)
	if err != nil {
		//报错下次重新连接
		err = groupCluster.Close()
		if err == nil {
			groupCluster = nil
		}

		return -1, err
	}

	resp := offResp.GetBlock(topic, partition)
	if resp == nil {
		return -1, errors.New("kafka not Block info")
	}

	return resp.Offset, nil
}
