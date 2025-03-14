package rmq

import (
	"container/list"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/streadway/amqp"
	"github.com/wyy8261/gmf/conf"
	"github.com/wyy8261/gmf/logger"
	"runtime"
	"sync"
	"time"
)

func init() {
	addr := make([]string, 0)
	addr = append(addr, getAmqpString(conf.Default().RabbitMQ.User, conf.Default().RabbitMQ.Pwd, fmt.Sprintf("%s:%d", conf.Default().RabbitMQ.IP, conf.Default().RabbitMQ.Port), "/"))

	//注册MQ连接
	err := InitConnection("Sender", addr, runtime.NumCPU())
	if err != nil {
		logger.LOGE("RegisterRMQ:", err)
		return
	}
	err = RegProducer("Sender")
	if err != nil {
		logger.LOGE("RegisterRMQ:", err)
		return
	}
	//启动RMQ异步生产者服务
	StartRmq()
}

type RmqInfo struct {
	ConnName   string
	Exchange   string
	RoutingKey string
	Body       []byte
	Headers    amqp.Table
}

type RmqManage struct {
	queue *list.List
	mutex sync.Mutex
	rouse chan int
	stop  bool
}

var (
	gRmqManage *RmqManage = &RmqManage{
		stop:  false,
		rouse: make(chan int),
		queue: list.New(),
	}
)

func PostQueue(t *RmqInfo) {
	gRmqManage.Enqueue(t)
}

func StopRmq() {
	gRmqManage.stop = true
}

func StartRmq() {
	gRmqManage.stop = false
	go work()
}

func catch(err error) {
	if r := recover(); r != nil {
		logger.LOGD("r:", r)

		switch x := r.(type) {
		case string:
			err = errors.New(x)
		case error:
			err = x
		default:
			err = errors.New("")
		}
	}
}

func work() {
	ticker := time.NewTicker(3 * time.Second)
	for {
		var ok bool = false
		select {
		case <-gRmqManage.rouse:
			ok = true
		case <-ticker.C:
		}
		if gRmqManage.stop {
			break
		}
		if ok {
			rmqTask()
		}
	}
}

func rmqTask() {
	defer func() {
		if r := recover(); r != nil {
			logger.LOGE("err:", r)
		}
	}()

	for {
		t, ok := gRmqManage.Dequeue()
		if ok {
			start := time.Now()
			err := Publish(t.ConnName, t.Exchange, t.RoutingKey, amqp.Publishing{
				DeliveryMode: amqp.Persistent,
				ContentType:  "text/plain",
				Body:         t.Body,
				Headers:      t.Headers,
			})
			if err != nil {
				logger.LOGE("err:", err)
			} else {
				logger.LOGD("(", time.Since(start), "),body:", string(t.Body))
			}
		} else {
			break
		}
	}
}

func (r *RmqManage) Enqueue(t *RmqInfo) {
	r.mutex.Lock()
	r.queue.PushBack(t)
	r.mutex.Unlock()
	select {
	case r.rouse <- 1:
	default:
	}
}

func (r *RmqManage) Dequeue() (*RmqInfo, bool) {
	var res *RmqInfo = nil

	r.mutex.Lock()
	defer r.mutex.Unlock()
	if r.queue.Len() > 0 {
		front := r.queue.Front()
		res = front.Value.(*RmqInfo)
		r.queue.Remove(front)
	} else {
		return nil, false
	}

	return res, true
}

func getAmqpString(user, pwd, url, vhost string) string {
	return fmt.Sprintf("amqp://%s:%s@%s%s", user, pwd, url, vhost)
}

// 外部使用

func RegisterConsumer(queueName string, callback func(msg amqp.Delivery)) {
	addr := make([]string, 0)
	addr = append(addr, getAmqpString(conf.Default().RabbitMQ.User, conf.Default().RabbitMQ.Pwd, fmt.Sprintf("%s:%d", conf.Default().RabbitMQ.IP, conf.Default().RabbitMQ.Port), "/"))
	//注册MQ连接
	err := InitConnection("Consumer", addr, runtime.NumCPU())
	if err != nil {
		logger.LOGE("RegisterRMQ:", err)
		return
	}
	//注册消费者队列
	err = AddMonitor("Consumer", queueName, callback)
	if err != nil {
		logger.LOGE("AddMonitor failed:", err)
		return
	}
	//注册为消费者，否则不能接收队列消息
	err = RegConsumer("Consumer")
	if err != nil {
		logger.LOGE("RegConsumer failed:", err)
		return
	}
	//启动RMQ消费者服务
	RunServer()
}

func Publish4CustomMsg(exchange, routingKey string, code int, data interface{}) {
	msg := struct {
		Code int         `json:"code"`
		Data interface{} `json:"data"`
	}{Code: code, Data: data}
	body, _ := json.Marshal(msg)

	PostQueue(&RmqInfo{
		ConnName:   "Sender",
		Exchange:   exchange,
		RoutingKey: routingKey,
		Body:       body,
	})
}

func Publish4CustomMsg_delay(exchange, routingKey string, delaySec, code int, data interface{}) {
	msg := struct {
		Code int         `json:"code"`
		Data interface{} `json:"data"`
	}{Code: code, Data: data}
	body, _ := json.Marshal(msg)

	PostQueue(&RmqInfo{
		ConnName:   "Sender",
		Exchange:   exchange,
		RoutingKey: routingKey,
		Body:       body,
		Headers:    amqp.Table{"x-delay": delaySec * 1000},
	})
}
