package export

type Exporter interface {
	GetEnrichedContent(uuid string) map[string]interface{}
}

type ContentExporter struct {

}

func (e *ContentExporter) GetEnrichedContent(uuid string) map[string]interface{} {
    //TODO implement logic 
	res := map[string]interface{}{"uuid": uuid}
	return res

}