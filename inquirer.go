package main

import (
	log "github.com/sirupsen/logrus"
	"strings"
)

const defaultDate = "0000-00-00"

type Inquirer interface {
	Inquire(collection string, candidates []string) (chan Content, error, int)
}

type MongoInquirer struct {
	Mongo DB
}

func (m *MongoInquirer) Inquire(collection string, candidates []string) (chan Content, error, int) {
	tx, err := m.Mongo.Open()

	if err != nil {
		return nil, err, 0
	}
	iter, length, err := tx.FindUUIDs(collection, candidates)
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
			docUUID, ok := result["uuid"]
			if !ok {
				log.Warnf("No uuid field found in iter result: %v. Skipping", result)
				continue
			}

			docs <- Content{docUUID.(string), getDate(result)}
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
	if date != "" {
		return
	}
	docPublishedDate, ok := result["publishedDate"]
	d, ok = docPublishedDate.(string)
	if ok {
		date = strings.Split(d, "T")[0]
	}
	if date != "" {
		return
	}

	return defaultDate
}
