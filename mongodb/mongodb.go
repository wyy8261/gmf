package mongodb

import (
	"context"
	"fmt"
	"github.com/wyy8261/go-simplelog"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/readpref"
	"time"
)

var (
	gClientOptions *options.ClientOptions = nil
)

func Init(dbhost, authdb, authuser, authpass string) {
	url := fmt.Sprintf("mongodb://%s:%s@%s/%s", authuser, authpass, dbhost, authdb)
	logger.LOGD("url:", url)
	gClientOptions = options.Client().ApplyURI(url)
}

func connect(db, collection string) (*mongo.Client, *mongo.Collection) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	client, err := mongo.Connect(ctx, gClientOptions)
	if err != nil {
		logger.LOGE("err:", err)
		return nil, nil
	}
	collec := client.Database(db).Collection(collection)
	return client, collec
}

func ping(client *mongo.Client) error {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	return client.Ping(ctx, readpref.Primary())
}

func close(client *mongo.Client) {
	err := client.Disconnect(nil)
	if err != nil {
		logger.LOGE("err:", err)
	}
}

func Count(db, collection string, query interface{}) (int, error) {
	ms, c := connect(db, collection)
	defer close(ms)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	count, err := c.CountDocuments(ctx, query)
	return int(count), err
}

func Insert(db, collection string, docs ...interface{}) error {
	ms, c := connect(db, collection)
	defer close(ms)

	mdocs := make([]interface{}, 0)
	mdocs = append(mdocs, docs...)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	_, err := c.InsertMany(ctx, docs)
	return err
}

func FindOne(db, collection string, query, selector, result interface{}) error {
	ms, c := connect(db, collection)
	defer close(ms)

	if query == nil {
		query = bson.M{}
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	res := c.FindOne(ctx, query)

	if res.Err() != nil {
		return res.Err()
	}

	return res.Decode(result)
}

func FindAll(db, collection string, query, selector, result interface{}) error {
	ms, c := connect(db, collection)
	defer close(ms)

	if query == nil {
		query = bson.M{}
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	cur, err := c.Find(ctx, query)
	if err != nil {
		return err
	}
	return cur.All(ctx, result)
}

func FindSortByCount(db, collection string, limit int, field string, query, result interface{}) (int, error) {
	ms, c := connect(db, collection)
	defer close(ms)

	if query == nil {
		query = bson.M{}
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	opts := options.Find()
	if []byte(field)[0] == '-' {
		SORT := bson.D{{string([]byte(field)[1:]), -1}}
		opts.SetSort(SORT)
	} else {
		SORT := bson.D{{field, 1}}
		opts.SetSort(SORT)
	}
	opts.SetLimit(int64(limit))

	cur, err := c.Find(ctx, query, opts)
	if err != nil {
		return 0, err
	}
	return 0, cur.All(ctx, result)
}

func Update(db, collection string, selector, update interface{}) error {
	ms, c := connect(db, collection)
	defer close(ms)

	if selector == nil {
		selector = bson.M{}
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	opts := options.Update().SetUpsert(false)
	_, err := c.UpdateOne(ctx, selector, update, opts)
	return err
}

func UpdateAll(db, collection string, selector, update interface{}) error {
	ms, c := connect(db, collection)
	defer close(ms)

	if selector == nil {
		selector = bson.M{}
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	opts := options.Update().SetUpsert(false)
	_, err := c.UpdateMany(ctx, selector, update, opts)
	return err
}

func Remove(db, collection string, selector interface{}) error {
	ms, c := connect(db, collection)
	defer close(ms)

	if selector == nil {
		selector = bson.M{}
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	_, err := c.DeleteOne(ctx, selector)
	return err
}

func RemoveAll(db, collection string, selector interface{}) error {
	ms, c := connect(db, collection)
	defer close(ms)

	if selector == nil {
		selector = bson.M{}
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	_, err := c.DeleteMany(ctx, selector)
	return err
}

func GetNextSequence(db, collection, name string) (int64, bool) {
	ms, c := connect(db, collection)
	defer close(ms)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	seq := struct {
		Value int64 `bson:"seqid"`
	}{Value: 1}

	opts := options.FindOneAndUpdate()
	opts.SetUpsert(true)

	res := c.FindOneAndUpdate(ctx, bson.M{"_id": name}, bson.M{"$inc": seq}, opts)
	err := res.Decode(&seq)
	if err != nil {
		logger.LOGE("err:", err)
	}
	return seq.Value + 1, true
}
