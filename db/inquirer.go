package db

import (
	"context"
	"github.com/coreos/fleet/log"
)

type Inquirer interface {
	Inquire(ctx context.Context, collection string) (chan string, error)
}

type MongoInquirer struct {
	Mongo DB
}

func (m *MongoInquirer) Inquire(ctx context.Context, collection string) (chan string, error) {
	tx, err := m.Mongo.Open()

	if err != nil {
		return nil, err
	}
	iter, length, err := tx.FindUUIDs(collection, 0, 100)
	if err != nil {
		tx.Close()
		return  nil, err
	}
	log.Infof("Nr of UUIDs found: %v", length)
	ids := make(chan string, 8)

	go func() {

		defer tx.Close()
		defer iter.Close()
		defer close(ids)


		var result map[string]interface{}

		for iter.Next(&result) {
			if err := ctx.Err(); err != nil {
				break
			}

			ids <- result["uuid"].(string)
		}
	}()

	return ids, nil
}
