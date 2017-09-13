package service

import (
	"bufio"
	"context"
	"fmt"
	"github.com/Financial-Times/content-exporter/content"
	"github.com/Financial-Times/content-exporter/db"
	"github.com/Financial-Times/transactionid-utils-go"
	log "github.com/Sirupsen/logrus"
	"net/http"
	"time"
)

type RequestHandler struct {
	Inquirer db.Inquirer
	Exporter content.Exporter
	Uploader content.Uploader
}

func (handler *RequestHandler) Export(writer http.ResponseWriter, request *http.Request) {
	defer request.Body.Close()

	tid := transactionidutils.GetTransactionIDFromRequest(request)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()

	docs, err := handler.Inquirer.Inquire(ctx, "content")
	if err != nil {
		msg := fmt.Sprintf(`Failed to read IDs from mongo for %v! "%v"`, "content", err.Error())
		log.Info(msg)
		http.Error(writer, msg, http.StatusServiceUnavailable)
		return
	}

	bw := bufio.NewWriter(writer)
	var failed []string
	for {
		doc, ok := <-docs
		if !ok {
			bw.WriteString(fmt.Sprintf("Failed uuids: %v\n", failed))
			bw.Flush()
			writer.(http.Flusher).Flush()
			break
		}

		payload, err := handler.Exporter.GetContent(doc.Uuid, tid)
		if err != nil {
			failed = append(failed, doc.Uuid)
			log.Errorf("Error by getting content for %v: %v\n", doc.Uuid, err)
			continue
		}

		err = handler.Uploader.Upload(payload, tid, doc.Uuid, doc.Date)
		if err != nil {
			failed = append(failed, doc.Uuid)
			log.Errorf("Error by uploading content for %v: %v\n", doc.Uuid, err)
			continue
		}

	}

}
