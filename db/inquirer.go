package db

import (
	"strings"
	"github.com/Financial-Times/content-exporter/service"
)

const defaultDate = "0000-00-00"

type Inquirer interface {
	Inquire(collection string) (chan service.Content, error, int)
}

type MongoInquirer struct {
	Mongo DB
}

func (m *MongoInquirer) Inquire(collection string) (chan service.Content, error, int) {
	tx, err := m.Mongo.Open()

	if err != nil {
		return nil, err, 0
	}
	iter, length, err := tx.FindUUIDs(collection)
	if err != nil {
		tx.Close()
		return nil, err, 0
	}

	docs := make(chan service.Content, 8)

	go func() {

		defer tx.Close()
		defer iter.Close()
		defer close(docs)

		var result map[string]interface{}

		for iter.Next(&result) {
			var uuid, date string
			docUUID, ok := result["uuid"]
			if ok {
				uuid = docUUID.(string)
			}
			docFirstPublishedDate, ok := result["firstPublishedDate"]
			d, ok := docFirstPublishedDate.(string)
			if ok {
				date = strings.Split(d, "T")[0]
			}
			docPublishedDate, ok := result["publishedDate"]
			d, ok = docPublishedDate.(string)
			if ok {
				date = strings.Split(d, "T")[0]
			}
			if date == "" {
				date = defaultDate
			}
			docs <- service.Content{uuid, date}
		}
	}()

	return docs, nil, length
}
