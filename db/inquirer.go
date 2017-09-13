package db

import (
	"context"
	"github.com/coreos/fleet/log"
	"strings"
)

const defaultDate = "0000-00-00"

type Inquirer interface {
	Inquire(ctx context.Context, collection string) (chan DBContent, error)
}

type MongoInquirer struct {
	Mongo DB
}

type DBContent struct {
	Uuid, Date string
}

func (m *MongoInquirer) Inquire(ctx context.Context, collection string) (chan DBContent, error) {
	tx, err := m.Mongo.Open()

	if err != nil {
		return nil, err
	}
	iter, length, err := tx.FindUUIDs(collection, 0, 100)
	if err != nil {
		tx.Close()
		return nil, err
	}
	log.Infof("Nr of UUIDs found: %v", length)
	docs := make(chan DBContent, 8)

	go func() {

		defer tx.Close()
		defer iter.Close()
		defer close(docs)

		var result map[string]interface{}

		for iter.Next(&result) {
			if err := ctx.Err(); err != nil {
				break
			}
			var uuid, date string
			docUUID, ok := result["uuid"]
			if ok {
				uuid = docUUID.(string)
			}
			docFirstPublishedDate, ok := result["firstPublishedDate"]
			if ok {
				date = strings.Split(docFirstPublishedDate.(string), "T")[0]
			}
			docPublishedDate, ok := result["publishedDate"]
			if ok {
				date = strings.Split(docPublishedDate.(string), "T")[0]
			}
			if date == "" {
				date = defaultDate
			}
			docs <- DBContent{uuid, date}
		}
	}()

	return docs, nil
}
