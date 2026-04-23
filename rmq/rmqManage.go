package rmq

import (
	"context"
	"time"

	"github.com/wyy8261/gmf/conf"
	"github.com/wyy8261/gmf/logger"
)

var (
	producerClient *RabbitMQ
	consumerClient *RabbitMQ
)

func defaultRabbitMQConfig() *conf.ServerInfo {
	cfg := conf.Default().RabbitMQ
	return &cfg
}

func ensureSender() (*RabbitMQ, error) {
	if producerClient != nil {
		return producerClient, nil
	}

	client, err := NewRabbitMQ(defaultRabbitMQConfig())
	if err != nil {
		return nil, err
	}

	producerClient = client
	return producerClient, nil
}

func ensureConsumer() (*RabbitMQ, error) {
	if consumerClient != nil {
		return consumerClient, nil
	}

	client, err := NewRabbitMQ(defaultRabbitMQConfig())
	if err != nil {
		return nil, err
	}

	consumerClient = client
	return consumerClient, nil
}
func PostQueue(exchangeName, routingKey, message string, delayTime ...time.Duration) {
	client, err := ensureSender()
	if err != nil {
		logger.LOGE("StartRmq:", err)
		return
	}
	client.PostQueue(exchangeName, routingKey, message, delayTime...)
}

// 外部使用
func RegisterConsumer(queueName string, callback Handler) *Subscription {
	client, err := ensureConsumer()
	if err != nil {
		logger.LOGE("RegisterRMQ:", err)
		return nil
	}

	subscription, err := client.Subscribe(context.Background(), queueName, callback)
	if err != nil {
		logger.LOGE("Subscribe failed:", err)
		client.Close()
		return nil
	}

	return subscription
}

func GetMessage(queueName string, size int, timeout ...time.Duration) (error, []string) {
	client, err := ensureConsumer()
	if err != nil {
		return err, nil
	}
	return client.GetMessage(queueName, size, timeout...)
}
