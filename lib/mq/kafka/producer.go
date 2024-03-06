package kafka

import (
	"errors"
	"github.com/Shopify/sarama"
	"github.com/maclon-lee/golanglib/lib/config"
	logger "github.com/maclon-lee/golanglib/lib/log"
	"os"
	"os/signal"
)

var producer sarama.AsyncProducer

/*
* 发送生产消息
*
* param  topic  主题
* param  text   发送消息内容
 */
func SendMessage(topic string, text []byte) error {
	var err error
	if producer == nil {
		cfglist := config.GetSubConfig("kafka")
		if cfglist != nil {
			address := cfglist.GetStringSlice("address")
			if len(address) == 0 {
				return errors.New("kafka address not config")
			}

			config := sarama.NewConfig()
			config.Producer.Flush.Messages = 2
			config.Producer.Return.Successes = false
			config.Producer.Return.Errors = true

			producer, err = sarama.NewAsyncProducer(address, config)
			if err != nil {
				return err
			}

			signals := make(chan os.Signal, 1)
			signal.Notify(signals, os.Interrupt)

			go func() {
			DoneLoop:
				for {
					select {
					case <-signals:
						closeSend()
						break DoneLoop
					}
				}
			}()
		} else {
			return errors.New("kafka not config")
		}
	}

	go func() {
		for err := range producer.Errors() {
			logger.Warnf(err.Error())
		}
	}()

	producer.Input() <- &sarama.ProducerMessage{Topic: topic, Key: nil, Value: sarama.ByteEncoder(text)}

	return nil
}

/*
* 关闭生产消息
 */
func closeSend() {
	if producer != nil {
		producer.AsyncClose()
	}
}
