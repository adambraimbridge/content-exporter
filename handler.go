package main

import (
	"bufio"
	"context"
	"fmt"
	"github.com/Financial-Times/content-exporter/db"
	log "github.com/Sirupsen/logrus"
	"net/http"
	"time"
	"github.com/Financial-Times/content-exporter/content"
	"github.com/Financial-Times/transactionid-utils-go"
)

type requestHandler struct {
	inquirer db.Inquirer
	exporter content.Exporter
	uploader content.Uploader
}

func (handler *requestHandler) export(writer http.ResponseWriter, request *http.Request) {
	defer request.Body.Close()

	tid := transactionidutils.GetTransactionIDFromRequest(request)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()

	docs, err := handler.inquirer.Inquire(ctx, "content")
	if err != nil {
		msg := fmt.Sprintf(`Failed to read IDs from mongo for %v! "%v"`, "content", err.Error())
		log.Info(msg)
		http.Error(writer, msg, http.StatusServiceUnavailable)
		return
	}
	
	bw := bufio.NewWriter(writer)
	for {
		doc, ok := <-docs
		if !ok {
			break
		}
		bw.WriteString("DEBUG: " + handler.uploader.(*content.S3Uploader).S3WriterURL + doc.Uuid + "?date=" + doc.Date + "\n")
		bw.Flush()
		writer.(http.Flusher).Flush()
		//TODO implement some retry mechanism
		payload, err := handler.exporter.GetContent(doc.Uuid, tid)
		if err != nil {
			log.Errorf("Error by getting content for %v: %v\n", doc.Uuid, err)
			continue
		}

		err = handler.uploader.Upload(payload, tid, doc.Uuid, doc.Date)
		if err != nil {
			log.Errorf("Error by uploading content for %v: %v\n", doc.Uuid, err)
			continue
		}

	}

}
