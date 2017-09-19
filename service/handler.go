package service

import (
	"encoding/json"
	"fmt"
	"github.com/Financial-Times/content-exporter/content"
	"github.com/Financial-Times/content-exporter/db"
	"github.com/Financial-Times/transactionid-utils-go"
	"github.com/gorilla/mux"
	"github.com/pborman/uuid"
	log "github.com/sirupsen/logrus"
	"net/http"
)

type RequestHandler struct {
	JobPool  Pool
	Inquirer db.Inquirer
	Exporter content.Exporter
	Uploader content.Uploader
	NrOfConcurrentWorkers int
}

func (handler *RequestHandler) Export(writer http.ResponseWriter, request *http.Request) {
	defer request.Body.Close()

	tid := transactionidutils.GetTransactionIDFromRequest(request)

	log.Infoln("Calling mongo")
	docs, err, count := handler.Inquirer.Inquire("content")
	if err != nil {
		msg := fmt.Sprintf(`Failed to read IDs from mongo for %v! "%v"`, "content", err.Error())
		log.Info(msg)
		http.Error(writer, msg, http.StatusServiceUnavailable)
		return
	}
	log.Infof("Nr of UUIDs found: %v", count)
	job := &job{ID: uuid.New(), DocIds: docs, Count: count, nrWorker: handler.NrOfConcurrentWorkers}
	handler.JobPool.AddJob(job)
	go job.Run(handler, tid, handler.HandleContent)

	writer.WriteHeader(http.StatusAccepted)
	writer.Header().Add("Content-Type", "application/json")

	err = json.NewEncoder(writer).Encode(job)
	if err != nil {
		msg := fmt.Sprintf(`Failed to write job %v to response writer: "%v"`, job.ID, err)
		log.Warn(msg)
		fmt.Fprintf(writer, "{\"ID\": \"%v\"}", job.ID)
		return
	}
}

func (handler *RequestHandler) GetJob(writer http.ResponseWriter, request *http.Request) {
	defer request.Body.Close()

	vars := mux.Vars(request)
	jobID := vars["jobID"]

	writer.Header().Add("Content-Type", "application/json")

	job, err := handler.JobPool.GetJob(jobID)
	if err != nil {
		msg := fmt.Sprintf(`{"message":"%v"}`, err)
		log.Info(msg)
		http.Error(writer, msg, http.StatusNotFound)
		return
	}

	err = json.NewEncoder(writer).Encode(job)
	if err != nil {
		msg := fmt.Sprintf(`Failed to write job %v to response writer: "%v"`, job.ID, err)
		log.Warn(msg)
		fmt.Fprintf(writer, "{\"ID\": \"%v\"}", job.ID)
		return
	}

}

func (handler *RequestHandler) HandleContent(tid string, doc db.Content) error {
	payload, err := handler.Exporter.GetContent(doc.Uuid, tid)
	if err != nil {
		log.Errorf("Error by getting content for %v: %v\n", doc.Uuid, err)
		return err
	}

	err = handler.Uploader.Upload(payload, tid, doc.Uuid, doc.Date)
	if err != nil {
		log.Errorf("Error by uploading content for %v: %v\n", doc.Uuid, err)
		return err
	}
	return nil
}
