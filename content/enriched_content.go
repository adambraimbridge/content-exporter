package content

import (
	"encoding/json"
	"fmt"
	"net/http"
	"github.com/Financial-Times/service-status-go/httphandlers"
)

type Client interface {
	Do(req *http.Request) (resp *http.Response, err error)
}

const enrichedContentPath = "/enrichedcontent/"

type Fetcher interface {
	GetContent(uuid, tid string) (map[string]interface{}, error)
}

type EnrichedContentFetcher struct {
	Client                 Client
	EnrichedContentBaseURL string
	XPolicyHeaderValues    string
	Authorization          string
}

func (e *EnrichedContentFetcher) GetContent(uuid, tid string) (map[string]interface{}, error) {
	req, err := http.NewRequest("GET", e.EnrichedContentBaseURL+enrichedContentPath+uuid, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Add("User-Agent", "UPP Content Exporter")
	req.Header.Add("Accept", "application/json")
	req.Header.Add("X-Request-Id", tid)

	if e.XPolicyHeaderValues != "" {
		req.Header.Add("X-Policy", e.XPolicyHeaderValues)
	}
	if e.Authorization != "" {
		req.Header.Add("Authorization", e.Authorization)
	}

	resp, err := e.Client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, fmt.Errorf("EnrichedContent returned HTTP %v", resp.StatusCode)
	}

	var result map[string]interface{}
	dec := json.NewDecoder(resp.Body)
	err = dec.Decode(&result)
	return result, err

}

func (e *EnrichedContentFetcher) Ping() (string, error) {
	req, err := http.NewRequest("GET", e.EnrichedContentBaseURL+httphandlers.GTGPath, nil)
	if err != nil {
		return "Error in building request to check if the varnish is good to go", err
	}

	resp, err := e.Client.Do(req)
	if err != nil {
		return "Varnish is not good to go.", err
	}
	if resp.StatusCode != http.StatusOK {
		return "Varnish is not good to go.", fmt.Errorf("GTG HTTP status code is %v", resp.StatusCode)
	}
	return "Varnish is good to go.", nil
}