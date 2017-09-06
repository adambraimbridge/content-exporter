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

		payload, err := handler.exporter.GetContent(doc.Uuid, tid)

		log.Infof("Error? [%v] This will be posted to generic-s3-writer: \n%v", err, payload)

		bw.WriteString(handler.uploader.(content.S3Uploader).S3WriterURL + doc.Uuid + "?publishedDate=" + doc.Date + "\n")

		bw.Flush()
		writer.(http.Flusher).Flush()
	}

}
