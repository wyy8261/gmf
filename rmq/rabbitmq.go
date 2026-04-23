package rmq

import (
	"container/list"
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/wyy8261/gmf/conf"

	"github.com/streadway/amqp"
	logger "github.com/wyy8261/gmf/logger"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
)

type MqMsg struct {
	Id         int    `gorm:"PRIMARY_KEY;AUTO_INCREMENT;NOT NULL;column:id"`
	Exchange   string `gorm:"column:exchange"`
	RoutingKey string `gorm:"column:routingKey"`
	Body       string `gorm:"column:body"`
	DelayMS    int    `gorm:"column:delay_ms"`
}

type QueueManage struct {
	queue *list.List
	mutex sync.Mutex
	rouse chan struct{}
	stop  bool
}

func (r *QueueManage) Enqueue(t *MqMsg) {
	r.mutex.Lock()
	r.queue.PushBack(t)
	r.mutex.Unlock()
	select {
	case r.rouse <- struct{}{}:
	default:
	}
}

func (r *QueueManage) Dequeue() (*MqMsg, bool) {
	var res *MqMsg = nil

	r.mutex.Lock()
	defer r.mutex.Unlock()
	if r.queue.Len() > 0 {
		front := r.queue.Front()
		res = front.Value.(*MqMsg)
		r.queue.Remove(front)
	} else {
		return nil, false
	}

	return res, true
}

type RabbitMQ struct {
	conn        *amqp.Connection
	channel     *amqp.Channel
	notifyClose chan *amqp.Error
	mutex       sync.Mutex // 防止并发重连
	producerMu  sync.Mutex
	reconnChan  chan struct{}
	queueManage QueueManage
	db          *gorm.DB
	errNum      int
	url         string
	producerOn  bool
}

type Message struct {
	Topic string
	Key   string
	Data  string
}

type Handler func(context.Context, *Message) error

func NewRabbitMQ(cfg *conf.ServerInfo) (*RabbitMQ, error) {
	if cfg == nil {
		return nil, fmt.Errorf("rabbitmq config is nil")
	}

	rmq := &RabbitMQ{}
	if err := rmq.Init(cfg); err != nil {
		return nil, err
	}

	return rmq, nil
}

const (
	producerWakeInterval  = 3 * time.Second
	reconnectInterval     = 5 * time.Second
	consumerRetryInterval = 3 * time.Second
	retryBatchSize        = 500
	retryTriggerCount     = 200
	getMessageInterval    = 100 * time.Millisecond
)

func (r *RabbitMQ) Init(cfg *conf.ServerInfo) error {
	var err error
	dsn := fmt.Sprintf("amqp://%s:%s@%s:%d/", cfg.User, cfg.Pwd, cfg.IP, cfg.Port)
	_, err = r.Connect(dsn)

	if err != nil {
		return fmt.Errorf("failed to connect to RabbitMQ: %w", err)
	}

	r.reconnChan = make(chan struct{}, 1)

	go r.handleReconnect(dsn)
	return nil
}

func (r *RabbitMQ) Connect(url string) (rabbitMq *RabbitMQ, err error) {
	r.url = url
	r.conn, err = amqp.Dial(url)
	if err != nil {
		return
	}

	r.channel, err = r.conn.Channel()
	if err != nil {
		return
	}

	r.notifyClose = make(chan *amqp.Error)
	r.channel.NotifyClose(r.notifyClose)

	return r, nil
}

func (r *RabbitMQ) reconnectLocked() error {
	if r.url == "" {
		return fmt.Errorf("rabbitmq url is empty")
	}
	if r.channel != nil {
		_ = r.channel.Close()
		r.channel = nil
	}
	if r.conn != nil {
		_ = r.conn.Close()
		r.conn = nil
	}

	conn, err := amqp.Dial(r.url)
	if err != nil {
		return err
	}

	channel, err := conn.Channel()
	if err != nil {
		_ = conn.Close()
		return err
	}

	r.conn = conn
	r.channel = channel
	r.notifyClose = make(chan *amqp.Error, 1)
	r.channel.NotifyClose(r.notifyClose)

	return nil
}

func (r *RabbitMQ) publishLocked(exchangeName, routingKey string, publishing amqp.Publishing) error {
	if r.channel == nil || r.conn == nil || r.conn.IsClosed() {
		if err := r.reconnectLocked(); err != nil {
			return err
		}
	}

	return r.channel.Publish(
		exchangeName,
		routingKey,
		true,
		false,
		publishing,
	)
}

func (r *RabbitMQ) consumeMessagesWithTagLocked(queueName, consumerTag string) (<-chan amqp.Delivery, error) {
	if r.channel == nil || r.conn == nil || r.conn.IsClosed() {
		if err := r.reconnectLocked(); err != nil {
			return nil, err
		}
	}

	if r.channel == nil {
		return nil, fmt.Errorf("channel is nil")
	}

	return r.channel.Consume(
		queueName,
		consumerTag,
		false,
		false,
		false,
		false,
		nil,
	)
}

func (r *RabbitMQ) handleReconnect(url string) {
	for {
		closeErr := <-r.notifyClose
		if closeErr != nil {
			logger.LOGE("RabbitMQ连接断开,开始尝试重连:", closeErr)
			for {
				time.Sleep(reconnectInterval)
				r.mutex.Lock()
				r.url = url
				reconnectErr := r.reconnectLocked()
				r.mutex.Unlock()
				if reconnectErr == nil {
					logger.LOGD("RabbitMQ重连成功")
					select {
					case r.reconnChan <- struct{}{}:
					default:
					}
					break
				}
				logger.LOGE("RabbitMQ重连失败,继续重试:", reconnectErr)
			}
		}
	}
}

func (r *RabbitMQ) producerWork() {
	var (
		ticker         = time.NewTicker(producerWakeInterval)
		opsSinceRetry  = 0
		connectHealthy = true
	)
	defer ticker.Stop()

	for {
		shouldDrain := false
		select {
		case <-r.queueManage.rouse:
			shouldDrain = true
		case <-ticker.C:
		}
		if r.queueManage.stop {
			break
		}
		if shouldDrain {
			for {
				msg, ok := r.queueManage.Dequeue()
				if !ok {
					break
				}

				err := r.publishQueuedMessage(msg)
				if err != nil {
					logger.LOGE("err:", err, ",Exchange:", msg.Exchange, ",body:", msg.Body)
					r.errNum++
					if r.db != nil {
						if dbErr := r.db.Create(msg).Error; dbErr != nil {
							logger.LOGE("persist mq message err:", dbErr)
						}
					}
					connectHealthy = false
				} else {
					connectHealthy = true
				}
				opsSinceRetry++
			}
		}
		if opsSinceRetry > retryTriggerCount {
			retriedCount := 0
			if connectHealthy && r.db != nil {
				for {
					var res []MqMsg
					err := r.db.Limit(retryBatchSize).Find(&res).Error
					if err == nil && res != nil && len(res) > 0 {
						ids := make([]int, 0, len(res))
						for _, item := range res {
							start := time.Now()
							err = r.publishQueuedMessage(&item)
							if err != nil {
								logger.LOGE("retryPost err:", err, ",Exchange:", item.Exchange, ",body:", item.Body)
								break
							}
							logger.LOGD("retryPost (", time.Since(start), "),Exchange:", item.Exchange, ",body:", item.Body)
							ids = append(ids, item.Id)
							retriedCount++
						}
						if len(ids) > 0 {
							err = deleteMqMsgByIDs(r.db, ids)
							if err != nil {
								logger.LOGE("err:", err)
							}
						}
					}
					if err != nil || res == nil || len(res) == 0 {
						break
					}
				}

			}
			if retriedCount > 0 {
				logger.LOGI("enter read sqlite connectStatus:", connectHealthy, ",r.db:", r.db != nil, ",errNum:", r.errNum, ",retryPost size:", retriedCount)
				r.errNum -= retriedCount
				if r.errNum < 0 {
					r.errNum = 0
				}
			}
			opsSinceRetry = 0
		}
	}
}

func (r *RabbitMQ) ensureProducer() error {
	r.producerMu.Lock()
	defer r.producerMu.Unlock()

	if r.producerOn {
		return nil
	}

	r.queueManage.queue = list.New()
	r.queueManage.stop = false
	r.queueManage.rouse = make(chan struct{}, 1)

	db, err := gorm.Open(sqlite.Open("gorm.db"), &gorm.Config{})
	if err != nil {
		logger.LOGE("err:", err)
		return err
	}
	if err := db.AutoMigrate(&MqMsg{}); err != nil {
		logger.LOGE("err:", err)
		return err
	}

	r.db = db
	r.producerOn = true
	go r.producerWork()

	return nil
}

// 根据ID列表删除记录
func deleteMqMsgByIDs(db *gorm.DB, ids []int) error {
	if len(ids) == 0 {
		return nil
	}
	result := db.Where("id IN (?)", ids).Delete(&MqMsg{})
	return result.Error
}

func (r *RabbitMQ) PostQueue(exchangeName, routingKey, message string, delayTime ...time.Duration) {
	if err := r.ensureProducer(); err != nil {
		logger.LOGE("init producer err:", err)
		return
	}

	msg := &MqMsg{
		Exchange:   exchangeName,
		RoutingKey: routingKey,
		Body:       message,
	}
	if len(delayTime) > 0 && delayTime[0] > 0 {
		msg.DelayMS = int(delayTime[0] / time.Millisecond)
	}

	r.queueManage.Enqueue(msg)
}

func (r *RabbitMQ) publishQueuedMessage(msg *MqMsg) error {
	r.mutex.Lock()
	defer r.mutex.Unlock()

	publishing := amqp.Publishing{
		DeliveryMode: amqp.Persistent,
		ContentType:  "text/plain",
		Body:         []byte(msg.Body),
	}
	if msg.DelayMS > 0 {
		publishing.Headers = amqp.Table{
			"x-delay": msg.DelayMS,
		}
	}

	return r.publishLocked(msg.Exchange, msg.RoutingKey, publishing)
}

func (r *RabbitMQ) GetMessage(queueName string, size int, timeout ...time.Duration) (error, []string) {
	res := make([]string, 0, size)
	if size <= 0 {
		return nil, res
	}

	waitTimeout := time.Duration(0)
	if len(timeout) > 0 && timeout[0] > 0 {
		waitTimeout = timeout[0]
	}

	var deadline time.Time
	if waitTimeout > 0 {
		deadline = time.Now().Add(waitTimeout)
	}

	for len(res) < size {
		r.mutex.Lock()
		if r.channel == nil || r.conn == nil || r.conn.IsClosed() {
			if err := r.reconnectLocked(); err != nil {
				r.mutex.Unlock()
				return fmt.Errorf("reconnect failed: %w", err), nil
			}
		}

		if r.channel == nil {
			r.mutex.Unlock()
			return fmt.Errorf("channel is nil after reconnect"), nil
		}

		msg, ok, err := r.channel.Get(queueName, true)
		r.mutex.Unlock()
		if err != nil {
			return fmt.Errorf("rabbitmq get failed: %w", err), res
		}
		if !ok {
			if waitTimeout <= 0 || time.Now().After(deadline) {
				break
			}
			time.Sleep(getMessageInterval)
			continue
		}

		res = append(res, string(msg.Body))

		if waitTimeout > 0 && time.Now().After(deadline) {
			break
		}
	}

	return nil, res
}

func (r *RabbitMQ) Subscribe(ctx context.Context, queue string, handler Handler) (*Subscription, error) {
	subscription := r.newSubscription(queue, handler)
	if err := subscription.start(ctx); err != nil {
		return nil, err
	}

	return subscription, nil
}

func (r *RabbitMQ) Close() {
	if r.producerOn {
		r.queueManage.stop = true
		select {
		case r.queueManage.rouse <- struct{}{}:
		default:
		}
	}
	if r.channel != nil {
		r.channel.Close()
	}
	if r.conn != nil {
		r.conn.Close()
	}
}

type Subscription struct {
	client      *RabbitMQ
	queue       string
	handler     Handler
	consumerTag string
	msgs        <-chan amqp.Delivery
	ctx         context.Context
	cancel      context.CancelFunc
	startMu     sync.Mutex
	started     bool
}

func (r *RabbitMQ) newSubscription(queue string, handler Handler) *Subscription {
	return &Subscription{
		client:      r,
		queue:       queue,
		handler:     handler,
		consumerTag: fmt.Sprintf("%s-%d", queue, time.Now().UnixNano()),
	}
}

func (c *Subscription) start(ctx context.Context) error {
	if ctx == nil {
		ctx = context.Background()
	}
	if c.handler == nil {
		return fmt.Errorf("handler is nil")
	}

	c.startMu.Lock()
	defer c.startMu.Unlock()
	if c.started {
		return nil
	}

	c.ctx, c.cancel = context.WithCancel(ctx)
	if err := c.restartConsume(); err != nil {
		return err
	}

	c.started = true
	go c.consumerWork()
	return nil
}

func (c *Subscription) restartConsume() error {
	c.client.mutex.Lock()
	defer c.client.mutex.Unlock()

	msgs, err := c.client.consumeMessagesWithTagLocked(c.queue, c.consumerTag)
	if err != nil {
		return fmt.Errorf("failed to register a consumer: %w", err)
	}

	c.msgs = msgs
	return nil
}

func (c *Subscription) consumerWork() {
	for {
		select {
		case <-c.ctx.Done():
			return
		default:
		}

		if c.msgs == nil {
			if err := c.restartConsume(); err != nil {
				logger.LOGE("consumer restart failed", c.queue, err)
				select {
				case <-c.ctx.Done():
					return
				case <-time.After(consumerRetryInterval):
				}
				continue
			}
		}

		for {
			select {
			case <-c.ctx.Done():
				return
			case msg, ok := <-c.msgs:
				if !ok { //连接已断开
					c.msgs = nil
					goto RESTART
				}

				message := &Message{
					Topic: c.queue,
					Key:   msg.RoutingKey,
					Data:  string(msg.Body),
				}
				if err := c.handler(c.ctx, message); err != nil {
					logger.LOGE("error", "consumer error", c.queue, msg.RoutingKey, msg.Body, err)
				}
				if err := msg.Ack(false); err != nil {
					logger.LOGE("ack error", c.queue, msg.RoutingKey, err)
				}
			}
		}
	RESTART:
	}
}

func (c *Subscription) Cancel() error {
	c.startMu.Lock()
	defer c.startMu.Unlock()

	if c.cancel != nil {
		c.cancel()
	}
	c.started = false
	c.msgs = nil

	if c.client.channel == nil {
		return fmt.Errorf("channel is nil")
	}

	return c.client.channel.Cancel(c.consumerTag, false)
}
