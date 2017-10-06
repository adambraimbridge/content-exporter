package content

import "strings"

type Stub struct {
	Uuid, Date string
}

const DefaultDate = "0000-00-00"

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

