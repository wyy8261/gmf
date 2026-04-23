package redis

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/wyy8261/gmf/logger"
	"github.com/wyy8261/gmf/util"

	rds "github.com/redis/go-redis/v9"
	"github.com/wyy8261/gmf/conf"
)

var (
	ctx    = context.Background()
	client *rds.Client
)

func Client() *rds.Client {
	if client != nil {
		return client
	}

	cfg := conf.Default().Redis
	client = rds.NewClient(&rds.Options{
		Addr:     cfg.Addr(),
		Password: cfg.Pwd,
		DB:       util.Atoi(cfg.DBName),
		PoolSize: 100,
	})
	logger.LOGD("addr:", cfg.Addr())
	return client
}

/* -------------------- 工具函数 -------------------- */

func normalizeValue(v interface{}) string {
	switch t := v.(type) {
	case string:
		return t
	case []byte:
		return string(t)
	case int, int8, int16, int32, int64:
		return fmt.Sprintf("%d", t)
	case uint, uint8, uint16, uint32, uint64:
		return fmt.Sprintf("%d", t)
	case float32:
		return strconv.FormatFloat(float64(t), 'f', -1, 32)
	case float64:
		return strconv.FormatFloat(t, 'f', -1, 64)
	default:
		return fmt.Sprint(t)
	}
}

func parseResult(val interface{}, dest interface{}) error {
	switch d := dest.(type) {
	case *string:
		*d = fmt.Sprint(val)
	case *[]byte:
		*d = []byte(fmt.Sprint(val))
	case *int:
		v, _ := strconv.Atoi(fmt.Sprint(val))
		*d = v
	case *int32:
		v, _ := strconv.Atoi(fmt.Sprint(val))
		*d = int32(v)
	case *int64:
		v, _ := strconv.ParseInt(fmt.Sprint(val), 10, 64)
		*d = v
	case *uint:
		v, _ := strconv.ParseUint(fmt.Sprint(val), 10, 64)
		*d = uint(v)
	case *uint32:
		v, _ := strconv.ParseUint(fmt.Sprint(val), 10, 64)
		*d = uint32(v)
	case *uint64:
		v, _ := strconv.ParseUint(fmt.Sprint(val), 10, 64)
		*d = v
	case *float32:
		v, _ := strconv.ParseFloat(fmt.Sprint(val), 32)
		*d = float32(v)
	case *float64:
		v, _ := strconv.ParseFloat(fmt.Sprint(val), 64)
		*d = v
	case *bool:
		*d = fmt.Sprint(val) == "1" || fmt.Sprint(val) == "true"
	default:
		return errors.New("参数错误")
	}
	return nil
}

/* -------------------- 基础 KV -------------------- */

func Set(key string, data interface{}) error {
	return Client().Set(ctx, key, data, 0).Err()
}

func SetEx(key string, sec int, data interface{}) error {
	return Client().Set(ctx, key, data, time.Duration(sec)*time.Second).Err()
}

func Get(key string, data interface{}) error {
	val, err := Client().Get(ctx, key).Result()
	if err != nil {
		return err
	}
	return parseResult(val, data)
}

func Del(key string) error {
	return Client().Del(ctx, key).Err()
}

func Exists(key string) (bool, error) {
	n, err := Client().Exists(ctx, key).Result()
	return n > 0, err
}

func Incrby(key string, num *int) error {
	v, err := Client().IncrBy(ctx, key, int64(*num)).Result()
	if err != nil {
		return err
	}
	*num = int(v)
	return nil
}

/* -------------------- HASH -------------------- */

func HSet(key, field string, data interface{}) error {
	return Client().HSet(ctx, key, field, data).Err()
}

func HGet(key, field string, data interface{}) error {
	val, err := Client().HGet(ctx, key, field).Result()
	if err != nil {
		return err
	}
	return parseResult(val, data)
}

func HGetAll(key string) (map[string]string, error) {
	return Client().HGetAll(ctx, key).Result()
}

func HDel(key, field string) error {
	return Client().HDel(ctx, key, field).Err()
}

/* -------------------- SET -------------------- */

func Sadd(key string, data interface{}) error {
	return Client().SAdd(ctx, key, normalizeValue(data)).Err()
}

func Sismember(key string, data interface{}) (bool, error) {
	return Client().SIsMember(ctx, key, normalizeValue(data)).Result()
}

/* -------------------- ZSET -------------------- */

func ZAdd(key string, score int64, data interface{}) error {
	return Client().ZAdd(ctx, key, rds.Z{
		Score:  float64(score),
		Member: normalizeValue(data),
	}).Err()
}

func ZIncrby(key string, score int64, data interface{}) error {
	return Client().ZIncrBy(ctx, key, float64(score), normalizeValue(data)).Err()
}

func ZScore(key string, member interface{}) (int64, error) {
	v, err := Client().ZScore(ctx, key, normalizeValue(member)).Result()
	return int64(v), err
}

func ZRevrank(key string, member interface{}) (int, error) {
	v, err := Client().ZRevRank(ctx, key, normalizeValue(member)).Result()
	return int(v), err
}

func ZRevrange(key string, start, stop int64) ([]string, error) {
	return Client().ZRevRange(ctx, key, start, stop).Result()
}

func ZCard(key string) (int, error) {
	v, err := Client().ZCard(ctx, key).Result()
	return int(v), err
}

func ZPopMin(key string, count int) (map[string]string, error) {
	res, err := Client().ZPopMin(ctx, key, int64(count)).Result()
	if err != nil {
		return nil, err
	}

	ret := make(map[string]string)
	for _, z := range res {
		ret[z.Member.(string)] = strconv.FormatFloat(z.Score, 'f', -1, 64)
	}
	return ret, nil
}

/* -------------------- SCAN -------------------- */

func Scan(cursor int, pattern string, count int) (int, []string) {
	var keys []string
	iter := Client().Scan(ctx, uint64(cursor), pattern, int64(count)).Iterator()
	for iter.Next(ctx) {
		keys = append(keys, iter.Val())
	}
	return 0, keys
}

/* -------------------- PUBSUB -------------------- */

func Subscribe(key string, proc func(data []byte)) error {
	sub := Client().Subscribe(ctx, key)
	ch := sub.Channel()
	for msg := range ch {
		proc([]byte(msg.Payload))
	}
	return nil
}

func Publish(key string, field string) error {
	return Client().Publish(ctx, key, field).Err()
}
