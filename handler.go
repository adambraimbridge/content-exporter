package main

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"github.com/Financial-Times/content-exporter/db"
	log "github.com/Sirupsen/logrus"
	"net/http"
	"time"
	"github.com/Financial-Times/content-exporter/export"
)

type requestHandler struct {
	inquirer db.Inquirer
	exporter export.Exporter
}

func (handler *requestHandler) export(writer http.ResponseWriter, request *http.Request) {
	defer request.Body.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()

	ids, err := handler.inquirer.Inquire(ctx, "content")
	if err != nil {
		msg := fmt.Sprintf(`Failed to read IDs from mongo for %v! "%v"`, "content", err.Error())
		log.Info(msg)
		http.Error(writer, msg, http.StatusServiceUnavailable)
		return
	}

	id := struct {
		Payload map[string]interface{} `json:"payload"`
	}{}

	bw := bufio.NewWriter(writer)
	for {
		//TODO call enrichedcontent for every id
		docID, ok := <-ids
		if !ok {
			break
		}

		payload := handler.exporter.GetEnrichedContent(docID)

		id.Payload = payload
		jd, _ := json.Marshal(id)

		bw.WriteString(string(jd) + "\n")

		bw.Flush()
		writer.(http.Flusher).Flush()
	}

}
