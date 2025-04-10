package rmq

/*
	对于消费者，操作步骤大致为：InitConnection、AddExchange、AddQueue、AddBinding、AddMonitor、RegCusumer、RunServer
	对于生产者，操作步骤大致为：InitConnection、AddExchange、AddQueue、AddBinding、RegProducer、Publish

	AddExchange、AddQueue、AddBinding分别为连接注册永久型exchange和queue以及添加绑定关系，如果已经存在其实可以不执行函数。
	执行此操作可以保障手动删除exchange和queue等意外操作导致的问题，程序会自动重新注册exchange和queue以保证
	程序继续执行。

	AddMonitor生产者忽略，对于消费者，需要由此传入回调函数。回调函数内，
	应手动执行消息的Ack函数，如：
	func MsgHandle(msg amqp.Delivery) {
		//do something with msg
		msg.Ack(false)
	}
*/

import (
	"errors"
	"github.com/wyy8261/gmf/logger"
	"log"
	"sync"
	"time"

	"github.com/streadway/amqp"
)

var (
	conns map[string]*RmqConn
)

func init() {
	conns = make(map[string]*RmqConn)
}

// RmqConn
type RmqConn struct {
	alias       string   //别名
	uris        []string //amqp格式化地址
	conn        *amqp.Connection
	notifyClose chan *amqp.Error
	ch          *amqp.Channel
	exchanges   []*Exchange
	queues      []*Queue
	bindings    []*Binding
	monitors    []*Monitor
	isConsumer  bool //注册了消费者后，不可用来发布消息
	maxOpt      int  //仅消费者有效
}

// Exchange 交换
type Exchange struct {
	name  string
	etype string
}

// Queue 队列
type Queue struct {
	name string
}

// Binding exchange和队列的绑定
type Binding struct {
	exchange  string
	queueName string
	key       string
}

// Monitor 消费者监听的队列和回调函数
type Monitor struct {
	queueName string
	callback  MsgCallback //消息的回调函数
	msgs      <-chan amqp.Delivery
}

// 消息处理函数
type MsgCallback func(amqp.Delivery)

//InitConnection 初始化RMQ连接，不区分用于发送还是接收。支持多个地址，断开后，会尝试重连
/*
connName 用来区分不同连接的别名
uris     AMQP格式化地址。多个地址时，重连自动使用下一个
maxOpt   同时最多处理消息数量,仅对消费者生效, 为0时不限制。
此连接下所有消费者共享，处理的消息在执行Ack之前，占用一个位置
*/
func InitConnection(connName string, uris []string, maxOpt int) error {
	if len(uris) == 0 {
		return errors.New("No uris")
	}
	_, ok := conns[connName]
	if ok {
		return errors.New("Connection exists:" + connName)
	}

	conn := &RmqConn{}
	conn.alias = connName
	conn.uris = uris
	conn.isConsumer = false

	//尝试连接
	err := conn.connect()
	if err != nil {
		return err
	}

	err = conn.ch.Qos(maxOpt, 0, true)
	if err != nil {
		return err
	}
	conn.maxOpt = maxOpt

	conns[connName] = conn

	//处理断线重连
	go func(conn *RmqConn) {
		conn.notifyClose = make(chan *amqp.Error)
		conn.conn.NotifyClose(conn.notifyClose)
		for {
			err := <-conn.notifyClose
			if err != nil {
				logger.LOGE("Connection closed: ", err)
				// 尝试重新连接
				for {
					if conn.connect() == nil {
						logger.LOGD("Reconnected to RabbitMQ")
						break
					}
					logger.LOGE("Failed to reconnect: ", err)
					time.Sleep(30 * time.Second) // 等待一段时间后重试
				}
			}
		}
	}(conn)

	return nil
}

/*
RegConsumer 注册为消费者，与生产者互斥。将为消费者做准备工作

@connName 同RegisterRmq中的connName参数。
*/
func RegConsumer(connName string) error {
	conn, ok := conns[connName]
	if !ok {
		return errors.New("No connection:" + connName)
	}

	conn.isConsumer = true
	return nil
}

// RunServer 启动消费者服务。 如果只注册了生产者，则不需要执行此函数。
// 注意：此函数非阻塞模式，如在main函数中执行，需要调用者保证main函数不退出
func RunServer() {
	for _, conn := range conns {
		if conn.isConsumer {
			go conn.run()
		}
	}
}

/*
AddExchange 添加交换,持久型，可以添加多个。
非必须，但为了防止首次使用以及手动删除等情况丢失数据，建议提前定义好。
*/
func AddExchange(connName, exchange, exchangeType string) error {
	conn, ok := conns[connName]
	if !ok {
		return errors.New("No connection:" + connName)
	}
	exc := new(Exchange)
	exc.name = exchange
	exc.etype = exchangeType

	err := exc.exchangeDeclare(conn.ch)
	if err != nil {
		return err
	}

	conn.exchanges = append(conn.exchanges, exc)
	return nil
}

func (exc *Exchange) exchangeDeclare(ch *amqp.Channel) error {
	err := ch.ExchangeDeclare(exc.name,
		exc.etype, true, false, false, false, nil)
	return err
}

/*
AddQueue 添加队列，持久型。可以添加多个。
非必须，但为了防止首次使用以及手动删除等情况丢失数据，建议提前定义好。
*/
func AddQueue(connName, queue string) error {
	conn, ok := conns[connName]
	if !ok {
		return errors.New("No connection:" + connName)
	}
	q := new(Queue)
	q.name = queue

	err := q.queueDeclare(conn.ch)
	if err != nil {
		return err
	}
	conn.queues = append(conn.queues, q)
	return nil
}

func (q *Queue) queueDeclare(ch *amqp.Channel) error {
	args := make(map[string]interface{})
	args["x-queue-type"] = "classic"
	_, err := ch.QueueDeclare(q.name,
		true, false, false, false, args)
	return err
}

/*
AddBinding  添加交换和队列的绑定信息，应该在exchange和queue定义后再执行，可加多个绑定关系。
生产者非必须，但为了防止首次使用以及手动删除等情况丢失数据，建议生成者和消费者都提前定义好。

@connName 连接别名，同RegisterRmq中的connName参数
@exchange 交换名称
@queue 队列名称
@key   交换和队列绑定的key, key影响队列接收的内容。
*/
func AddBinding(connName, exchange, queue, key string) error {
	conn, ok := conns[connName]
	if !ok {
		return errors.New("No connection:" + connName)
	}

	binding := new(Binding)
	binding.exchange = exchange
	binding.queueName = queue
	binding.key = key

	err := binding.bind(conn.ch)
	if err != nil {
		return err
	}

	conn.bindings = append(conn.bindings, binding)
	return nil
}

// bind 将exchange和queue绑定
func (b *Binding) bind(ch *amqp.Channel) error {
	err := ch.QueueBind(b.queueName, b.key,
		b.exchange, false, nil)

	return err
}

/*
AddMonitor 添加监听队列和回调函数。 对于生产者，此函数将无意义

@connName 连接别名，同RegisterRmq中的connName参数
@queue    监听队列名称
@callback 回调函数, 用于处理具体消息内容， 应在该函数用执行Ack
*/
func AddMonitor(connName, queue string, callback MsgCallback) error {
	conn, ok := conns[connName]
	if !ok {
		return errors.New("No connection:" + connName)
	}
	if callback == nil {
		return errors.New("AddMonitor with nil callback")
	}

	monitor := &Monitor{}
	monitor.queueName = queue
	monitor.callback = callback

	err := monitor.consume(conn.ch)
	if err != nil {
		return err
	}
	conn.monitors = append(conn.monitors, monitor)

	return nil
}

func (m *Monitor) consume(ch *amqp.Channel) error {
	//注册消费者
	msgs, err := ch.Consume(
		m.queueName, // queue
		"",          // consumer  此参数已被取消
		false,       // auto-ack
		false,       // exclusive
		false,       // no-local
		false,       // no-wait
		nil,         // args
	)

	if err != nil {
		log.Println("Consume failed:", err)
		return err
	}
	m.msgs = msgs
	return nil
}

/*
RegProducer 注册生产者
connName 同RegisterRmq中的connName参数
*/
func RegProducer(connName string) error {
	conn, ok := conns[connName]
	if !ok {
		return errors.New("No connection:" + connName)
	}

	//注册消费者后，不可再作为生产者使用
	if conn.isConsumer {
		return errors.New(connName + " has been registered as consumer")
	}

	conn.isConsumer = false
	return nil
}

/*
Publish 发布消息

参数：
connName 连接别名，同RegisterRmq中的connName参数
exchange 交换名称
routingKey 路由key, 根据绑定交换和队列的key的匹配，发送至对应队列
info 发送的具体内容。因amqp.Publishing参数太多，未加封装，可根据需要添加必要的参数
*/
func Publish(connName, exchange, routingKey string,
	info amqp.Publishing) error {
	conn, ok := conns[connName]
	if !ok {
		return errors.New("No connection:" + connName)
	}
	if conn.isConsumer {
		return errors.New(connName + " is consumer")
	}

	if conn.isClosed() {
		err := conn.connect()
		if err != nil {
			return err
		}
	}

	err := conn.ch.Publish(exchange, routingKey, true, false, info)

	if err != nil {
		conn.close()
		//尝试重连,失败则退出.但仍然返回最初的错误
		err2 := conn.connect()
		if err2 != nil {
			return err
		}
		err2 = conn.prepareExchangeQueue()
		if err2 != nil {
			//打印重连日志
			log.Println(err2)
			return err
		}

		err = conn.ch.Publish(exchange, routingKey, true, false, info)
	}

	return err
}

func (connection *RmqConn) isClosed() bool {
	return connection.conn.IsClosed()
}

func (connection *RmqConn) connect() error {
	var i int
	var err error
	uriSize := len(connection.uris)

Loop:
	for i = 0; i < uriSize; i++ {
		conn, err := amqp.Dial(connection.uris[i])
		if err == nil {
			connection.conn = conn
			break Loop
		}
		//错误日志，因最终返回的错误信息只是最后一个连接的，在这里打印出所有的错误信息
		log.Println(err)
	}
	//所有连接都失败
	if err != nil {
		return err
	}

	connection.ch, err = connection.conn.Channel()
	if err != nil {
		connection.conn.Close()
		connection.conn = nil
		return err
	}

	//连接后重新设置监听通知
	for _, monitor := range connection.monitors {
		monitor.consume(connection.ch)
	}

	return nil
}

func (connection *RmqConn) close() error {
	connection.ch.Close()
	connection.conn.Close()

	return nil
}

func (connection *RmqConn) qos() error {
	return (connection.ch.Qos(connection.maxOpt, 0, true))
}

// 准备所有注册在此连接名下的exchange、queue和绑定关系
func (connection *RmqConn) prepareExchangeQueue() error {
	for _, exc := range connection.exchanges {
		err := exc.exchangeDeclare(connection.ch)
		if err != nil {
			return err
		}
	}

	for _, q := range connection.queues {
		err := q.queueDeclare(connection.ch)
		if err != nil {
			return err
		}
	}

	for _, binding := range connection.bindings {
		err := binding.bind(connection.ch)
		if err != nil {
			return err
		}
	}

	for _, monitor := range connection.monitors {
		err := monitor.consume(connection.ch)
		if err != nil {
			return err
		}
	}
	return nil
}

func (connection *RmqConn) run() {
	if !connection.isConsumer {
		return
	}

	var stop chan bool
	var wg sync.WaitGroup
	var err error
	var firstRun = true

	//进入循环，断开则重连.如果其中一个线程意外结束，通知其他线程结束重连
	for {
		if firstRun {
			firstRun = false
		} else {
			connection.close()
			time.Sleep(time.Second * 3)

			//连接
			err = connection.connect()
			if err != nil {
				log.Println("connect failed:", err)
				continue
			}
			err = connection.qos()
			if err != nil {
				log.Println("qos failed:", err)
				continue
			}

			err = connection.prepareExchangeQueue()
			if err != nil {
				log.Println("prepareExchangeQueue failed:", err)
				continue
			}

			//重连成功
			log.Println("重连成功")
		}

		stop = make(chan bool, len(connection.monitors))

		for _, monitor := range connection.monitors {
			wg.Add(1)
			go monitor.run(stop, &wg)
		}
		//网络中断或者其他原因导致其中一个断开，最终所有都会结束
		wg.Wait()
		close(stop)
		stop = nil

		log.Println("所有协程已退出")
	}
}

func (m *Monitor) run(stop chan bool, wg *sync.WaitGroup) {
	//无回调函数，直接返回但不发送结束消息
	if m.callback == nil {
		wg.Done()
		return
	}
Loop:
	for {
		select {
		case <-stop:
			log.Println("因其他协程原因退出")
			break Loop
		case msg := <-m.msgs:
			if msg.Acknowledger != nil {
				//callback执行完之前，应该执行msg.Ack(false)
				go m.callback(msg)
			} else {
				log.Println(m.queueName, "监听被关闭")
				break Loop
			}
			//default:
			//	time.Sleep(time.Millisecond * 10)
		}
	}
	stop <- true
	wg.Done()
}

func Close() {
	for _, conn := range conns {
		_ = conn.close()
	}
}
