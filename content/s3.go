package content

import "net/http"

type Uploader interface {

}

type S3Uploader struct {
	Client             *http.Client
	S3WriterURL string
}