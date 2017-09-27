package main

import (
	log "github.com/sirupsen/logrus"
	"strings"
)

const defaultDate = ""

type Inquirer interface {
	Inquire(collection string) (chan Content, error, int)
}

type MongoInquirer struct {
	Mongo DB
}

func (m *MongoInquirer) Inquire(collection string) (chan Content, error, int) {
	tx, err := m.Mongo.Open()

	if err != nil {
		return nil, err, 0
	}
	iter, length, err := tx.FindUUIDs(collection)
	if err != nil {
		tx.Close()
		return nil, err, 0
	}

	docs := make(chan Content, 8)

	go func() {

		defer tx.Close()
		defer iter.Close()
		defer close(docs)

		var result map[string]interface{}
		counter := 0
		for iter.Next(&result) {
			counter++
			var uuid string
			docUUID, ok := result["uuid"]
			if ok {
				uuid = docUUID.(string)
			}

			docs <- Content{uuid, getDate(result)}
		}
		log.Infof("Processed %v docs", counter)
	}()

	return docs, nil, length
}

func getDate(result map[string]interface{}) (date string) {
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
	return
}
