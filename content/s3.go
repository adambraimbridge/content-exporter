package content

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
)

const s3WriterPath = "/content/"

type Uploader interface {
	Upload(content map[string]interface{}, tid, uuid, date string) error
}

type S3Uploader struct {
	Client          Client
	S3WriterBaseURL string
}

func (u *S3Uploader) Upload(content map[string]interface{}, tid, uuid, date string) error {
	buf := new(bytes.Buffer)
	err := json.NewEncoder(buf).Encode(content)
	if err != nil {
		return err
	}

	req, err := http.NewRequest("PUT", u.S3WriterBaseURL+s3WriterPath+uuid+"?date="+date, buf)
	if err != nil {
		return err
	}
	req.Header.Add("User-Agent", "UPP Content Exporter")
	req.Header.Add("Content-Type", "application/json")
	req.Header.Add("X-Request-Id", tid)

	resp, err := u.Client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("Content RW S3 returned HTTP %v", resp.StatusCode)
	}

	return nil
}
