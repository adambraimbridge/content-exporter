package main

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"github.com/Financial-Times/content-exporter/service"
	log "github.com/Sirupsen/logrus"
	"net/http"
	"time"
)

type requestHandler struct {
	inquirer service.Inquirer
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
		ID string `json:"id"`
	}{}

	bw := bufio.NewWriter(writer)
	for {
		docID, ok := <-ids
		if !ok {
			break
		}

		id.ID = docID
		jd, _ := json.Marshal(id)

		bw.WriteString(string(jd) + "\n")

		bw.Flush()
		writer.(http.Flusher).Flush()
	}

}
