package content

import (
	log "github.com/sirupsen/logrus"
)


type Exporter struct {
	Fetcher  Fetcher
	Uploader Updater
}

func (e *Exporter) HandleContent(tid string, doc Stub) error {
	payload, err := e.Fetcher.GetContent(doc.Uuid, tid)
	if err != nil {
		log.Errorf("Error by getting content for %v: %v\n", doc.Uuid, err)
		return err
	}

	err = e.Uploader.Upload(payload, tid, doc.Uuid, doc.Date)
	if err != nil {
		log.Errorf("Error by uploading content for %v: %v\n", doc.Uuid, err)
		return err
	}
	return nil
}
