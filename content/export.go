package content

import (
	"strings"
	"fmt"
)

const DefaultDate = "0000-00-00"

type Stub struct {
	Uuid, Date string
}

type Exporter struct {
	Fetcher Fetcher
	Updater Updater
}

func NewExporter(fetcher Fetcher, updater Updater) *Exporter {
	return &Exporter{
		Fetcher: fetcher,
		Updater: updater,
	}
}

func (e *Exporter) HandleContent(tid string, doc Stub) error {
	payload, err := e.Fetcher.GetContent(doc.Uuid, tid)
	if err != nil {
		return fmt.Errorf("Error by getting content for %v: %v", doc.Uuid, err)
	}

	err = e.Updater.Upload(payload, tid, doc.Uuid, doc.Date)
	if err != nil {
		return fmt.Errorf("Error by uploading content for %v: %v", doc.Uuid, err)
	}
	return nil
}

func GetDate(result map[string]interface{}) (date string) {
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

	return DefaultDate
}
