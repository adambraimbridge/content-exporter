package content

import "net/http"

type HttpClient interface {
	Do(req *http.Request) (resp *http.Response, err error)
}
