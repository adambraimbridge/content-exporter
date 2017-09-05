package service

import (
	"context"
	"sync"
	"time"

	log "github.com/Sirupsen/logrus"
	"gopkg.in/mgo.v2"
)

var expectedConnections = 1
var connections = 0

const sortByDate = "-content.lastModified"

type Content struct {
	Body        map[string]interface{} `bson:"content"`
	ContentType string                 `bson:"content-type"`
}

// DB contains database functions
type DB interface {
	Open() (TX, error)
	Close()
}

// TX contains database transaction functions
type TX interface {
	FindUUIDs(collectionId string, skip int, batchsize int) (DBIter, int, error)
	Ping(ctx context.Context) error
	Close()
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

// FindUUIDs returns all uuids for a collection sorted by lastodified date, if no lastmodified exists records are returned at the end of the list
func (tx *MongoTX) FindUUIDs(collectionID string, skip int, batchsize int) (DBIter, int, error) {
	collection := tx.session.DB("upp-store").C(collectionID)
	log.Infof("Collection: %v", collection)

	query, projection := findUUIDsQueryElements()
	find := collection.Find(query).Select(projection).Batch(batchsize)

	if skip > 0 {
		find.Skip(skip)
	}

	count, err := find.Count()
	return find.Iter(), count + skip, err // add count to skip as this correctly computes the total size of the cursor
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

type DBIter interface {
	Done() bool
	Next(result interface{}) bool
	Err() error
	Timeout() bool
	Close() error
}
