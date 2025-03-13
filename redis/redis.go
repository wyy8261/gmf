package redis

import (
	"errors"
	"fmt"
	"github.com/gomodule/redigo/redis"
	"gmf/conf"
	"time"
)

var (
	pool *redis.Pool = nil
)

func getPool() *redis.Pool {
	if pool == nil {
		pool = newPool()
	}
	return pool
}
func newPool() *redis.Pool {
	return &redis.Pool{
		MaxIdle:     3,
		MaxActive:   10,
		IdleTimeout: 240 * time.Second,
		Dial: func() (redis.Conn, error) {
			c, err := redis.Dial("tcp", conf.Default().Redis.Addr())
			if err != nil {
				return nil, err
			}

			sPass := conf.Default().Redis.Pwd
			if len(sPass) > 0 {
				if _, err := c.Do("AUTH", sPass); err != nil {
					c.Close()
					return nil, err
				}
			}
			return c, err
		},
		TestOnBorrow: func(c redis.Conn, t time.Time) error {
			if time.Since(t) < time.Minute {
				return nil
			}
			_, err := c.Do("PING")
			return err
		},
	}
}

func parseReply(reply interface{}, err error, data interface{}) error {
	var (
		iTmp int     = 0
		fTmp float64 = 0
		uTmp uint64  = 0
	)
	switch data.(type) {
	case *string:
		*data.(*string), err = redis.String(reply, err)
	case *int:
		*data.(*int), err = redis.Int(reply, err)
	case *int32:
		iTmp, err = redis.Int(reply, err)
		*data.(*int32) = int32(iTmp)
	case *int64:
		*data.(*int64), err = redis.Int64(reply, err)
	case *uint:
		uTmp, err = redis.Uint64(reply, err)
		*data.(*uint) = uint(uTmp)
	case *uint32:
		uTmp, err = redis.Uint64(reply, err)
		*data.(*uint32) = uint32(uTmp)
	case *uint64:
		*data.(*uint64), err = redis.Uint64(reply, err)
	case *float32:
		fTmp, err = redis.Float64(reply, err)
		*data.(*float32) = float32(fTmp)
	case *float64:
		*data.(*float64), err = redis.Float64(reply, err)
	case *bool:
		*data.(*bool), err = redis.Bool(reply, err)
	default:
		return errors.New("参数错误")
	}
	return nil
}

func Set(key string, data interface{}) error {
	conn := getPool().Get()
	defer conn.Close()
	_, err := conn.Do("SET", key, data)
	if err != nil {
		return err
	}
	return nil
}

func SetEx(key string, sec int, data interface{}) error {
	conn := getPool().Get()
	defer conn.Close()
	_, err := conn.Do("SETEX", key, sec, data)
	if err != nil {
		return err
	}
	return nil
}

func Sadd(key string, data interface{}) error {
	conn := getPool().Get()
	defer conn.Close()
	_, err := conn.Do("SADD", key, data)
	if err != nil {
		return err
	}
	return nil
}

func Sismember(key string, data interface{}) (bool, error) {
	conn := getPool().Get()
	defer conn.Close()
	v, err := conn.Do("SISMEMBER", key, data)
	var ret bool
	err = parseReply(v, err, &ret)
	return ret, err
}

func Exists(key string) (bool, error) {
	conn := getPool().Get()
	defer conn.Close()
	v, err := conn.Do("EXISTS", key)
	var ret bool
	err = parseReply(v, err, &ret)
	return ret, err
}

func Get(key string, data interface{}) error {
	conn := getPool().Get()
	defer conn.Close()

	v, err := conn.Do("GET", key)
	err = parseReply(v, err, data)

	if err != nil {
		return err
	}
	return nil
}

func Del(key string) error {
	conn := getPool().Get()
	defer conn.Close()
	_, err := conn.Do("DEL", key)
	return err
}

func Incrby(key string, num *int) error {
	conn := getPool().Get()
	defer conn.Close()
	v, err := conn.Do("INCRBY", key, *num)
	err = parseReply(v, err, num)
	if err != nil {
		return err
	}
	return nil
}

func HSet(key string, field string, data interface{}) error {
	conn := getPool().Get()
	defer conn.Close()
	_, err := conn.Do("HSET", key, field, data)
	if err != nil {
		fmt.Println(err)
		return err
	}
	return nil
}
func HGet(key string, field string, data interface{}) error {
	conn := getPool().Get()
	defer conn.Close()
	v, err := conn.Do("HGET", key, field)
	err = parseReply(v, err, data)
	if err != nil {
		return err
	}
	return nil
}
func HGetAll(key string) (map[string]string, error) {
	conn := getPool().Get()
	defer conn.Close()
	return redis.StringMap(conn.Do("HGETALL", key))
}

func HDel(key string, field string) error {
	conn := getPool().Get()
	defer conn.Close()
	_, err := conn.Do("HDEL", key, field)
	return err
}

func ZAdd(key string, score int64, data interface{}) error {
	conn := getPool().Get()
	defer conn.Close()
	_, err := conn.Do("ZADD", key, score, data)
	if err != nil {
		fmt.Println(err)
		return err
	}
	return nil
}

func ZCard(key string) (int, error) {
	conn := getPool().Get()
	defer conn.Close()
	return redis.Int(conn.Do("ZCARD", key))
}

func ZPopMin(key string, count int) (map[string]string, error) {
	conn := getPool().Get()
	defer conn.Close()
	if count <= 0 {
		return redis.StringMap(conn.Do("ZPOPMIN", key))
	} else {
		return redis.StringMap(conn.Do("ZPOPMIN", key, count))
	}
}

func ZScore(key string, member interface{}) (int, error) {
	conn := getPool().Get()
	defer conn.Close()
	return redis.Int(conn.Do("ZSCORE", key, member))
}

func Scan(cursor int, pattern string, count int) (int, []string) {
	var (
		keys = []string{}
	)

	conn := getPool().Get()
	defer conn.Close()
	reply, err := redis.Values(conn.Do("SCAN", cursor, "MATCH", pattern, "COUNT", count))
	if err != nil {
		fmt.Println("SCAN command failed: %v", err)
		return 0, keys
	}
	// 解析 SCAN 结果
	_, err = redis.Scan(reply, &cursor, &keys)
	if err != nil {
		fmt.Println("Failed to parse SCAN result: %v", err)
	}
	return cursor, keys
}

// 订阅
func Subscribe(key string, proc func(data []byte)) error {
	conn := getPool().Get()
	defer conn.Close()
	psc := redis.PubSubConn{Conn: conn}
	psc.Subscribe(key)
	for {
		switch v := psc.Receive().(type) {
		case redis.Message:
			fmt.Printf("%s: message: %s\n", v.Channel, v.Data)
			proc(v.Data)
		case redis.Subscription:
			fmt.Printf("%s: %s %d\n", v.Channel, v.Kind, v.Count)
		case error:
			return v
		}
	}
}

// 发布
func Publish(key string, field string) error {
	conn := getPool().Get()
	defer conn.Close()
	_, err := conn.Do("PUBLISH", key, field)
	if err != nil {
		fmt.Println(err)
		return err
	}
	return nil
}
