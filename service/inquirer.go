package service

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
	log.Info("Mongo is opened: %+v", tx)
	ids := make(chan string, 8)

	go func() {
		iter, length, err := tx.FindUUIDs(collection, 0, 100)
		defer tx.Close()
		defer iter.Close()
		defer close(ids)
		if err != nil {
			log.Infof("Error in find uuids: %v", err)
			return
		}
		log.Infof("Nr of UUIDs found: %v", length)

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
