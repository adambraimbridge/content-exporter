package db

import (
	"context"
	"sync"
	"time"
	"gopkg.in/mgo.v2/bson"
	log "github.com/sirupsen/logrus"
	"gopkg.in/mgo.v2"
)

var expectedConnections = 1
var connections = 0

// DB contains database functions
type DB interface {
	Open() (TX, error)
	Close()
}

// TX contains database transaction functions
type TX interface {
	FindUUIDs(collectionId string) (Iterator, int, error)
	Ping(ctx context.Context) error
	Close()
}

type Iterator interface {
	Done() bool
	Next(result interface{}) bool
	Err() error
	Timeout() bool
	Close() error
}

// MongoTX wraps a mongo session
type MongoTX struct {
	session *mgo.Session
}

// MongoDB wraps a mango mongo session
type MongoDB struct {
	Urls    string
	Timeout int
	lock    *sync.Mutex
	session *mgo.Session
}

func NewMongoDatabase(connection string, timeout int) DB {
	return &MongoDB{Urls: connection, Timeout: timeout, lock: &sync.Mutex{}}
}

func (db *MongoDB) Open() (TX, error) {
	db.lock.Lock()
	defer db.lock.Unlock()

	if db.session == nil {
		session, err := mgo.DialWithTimeout(db.Urls, time.Duration(db.Timeout)*time.Millisecond)
		if err != nil {
			log.WithError(err).Error("Session error")
			return nil, err
		}
		session.SetSocketTimeout(10 * time.Minute)
		db.session = session
		connections++

		if connections > expectedConnections {
			log.Warnf("There are more MongoDB connections opened than expected! Are you sure this is what you want? Open connections: %v, expected %v.", connections, expectedConnections)
		}
	}

	return &MongoTX{db.session.Copy()}, nil
}

func (tx *MongoTX) FindUUIDs(collectionID string) (Iterator, int, error) {
	collection := tx.session.DB("upp-store").C(collectionID)

	query, projection := findUUIDsQueryElements()
	find := collection.Find(query).Select(projection).Batch(100).Limit(800000) //TODO remove limit

	count, err := find.Count() //after count returns new data may be added
	iter := find.Iter()
	countAfter, _ := find.Count()
	log.Printf("Nr of uuids found AFTER iter: %v", countAfter)
	return iter, count, err
}

// Ping returns a mongo ping response
func (tx *MongoTX) Ping(ctx context.Context) error {
	ping := make(chan error, 1)
	go func() {
		ping <- tx.session.Ping()
	}()

	select {
	case <-ctx.Done():
		return ctx.Err()
	case err := <-ping:
		return err
	}
}

// Close closes the transaction
func (tx *MongoTX) Close() {
	tx.session.Close()
}

// Close closes the entire database connection
func (db *MongoDB) Close() {
	db.session.Close()
}

var uuidProjection = bson.M{
	"uuid":               1,
	"firstPublishedDate": 1,
	"publishedDate":      1,
}

func findUUIDsQueryElements() (bson.M, bson.M) {
	return bson.M{
		"$and": []bson.M{
			{"$or": []bson.M{
				{"canBeDistributed": "yes"},
				{"canBeDistributed": bson.M{"$exists": false}},
			}},
			{"$or": []bson.M{
				{"type": "Article"},
				{"body": bson.M{"$ne": nil}},
				{"realtime": true},
			}},
		},
	}, uuidProjection
}