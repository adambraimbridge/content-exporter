package export

import (
	"net/http"
	"fmt"
	"encoding/json"
)

type Exporter interface {
	GetEnrichedContent(uuid, tid string) (map[string]interface{}, error)
}

type ContentExporter struct {
	Client *http.Client
	AppURL string
}

func (e *ContentExporter) GetEnrichedContent(uuid, tid string) (map[string]interface{}, error) {
	req, err := http.NewRequest("GET", e.AppURL+ uuid, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Add("User-Agent", "UPP Content Exporter")
	req.Header.Add("Accept", "application/json")
	req.Header.Add("X-Request-Id", tid)

	resp, err := e.Client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 && resp.StatusCode >= 300 {
		return nil, fmt.Errorf("EnrichedContent returned HTTP %v", resp.StatusCode)
	}

	var result map[string]interface{}
	dec := json.NewDecoder(resp.Body)
	err = dec.Decode(&result)
	return result, err

}