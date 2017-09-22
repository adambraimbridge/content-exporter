package content

import (
	"encoding/json"
	"fmt"
	"net/http"
)

type Client interface {
	Do(req *http.Request) (resp *http.Response, err error)
}


type Fetcher interface {
	GetContent(uuid, tid string) (map[string]interface{}, error)
}

type EnrichedContentFetcher struct {
	Client              Client
	EnrichedContentURL  string
	XPolicyHeaderValues string
	Authorization       string
}

func (e *EnrichedContentFetcher) GetContent(uuid, tid string) (map[string]interface{}, error) {
	req, err := http.NewRequest("GET", e.EnrichedContentURL+uuid, nil)
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
