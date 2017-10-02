package main

import (
	"encoding/json"
	"fmt"
	"github.com/Financial-Times/transactionid-utils-go"
	"github.com/gorilla/mux"
	"github.com/pborman/uuid"
	log "github.com/sirupsen/logrus"
	"net/http"
	"time"
)

type Locker struct {
	locked chan bool
	acked  chan struct{}
	quit   chan struct{}
}

type RequestHandler struct {
	FullExporter *FullExporter
	Inquirer     Inquirer
	Locker
}

func NewRequestHandler(fullExporter *FullExporter, mongo DB) *RequestHandler {
	lockedCh := make(chan bool)
	ackedCh := make(chan struct{})
	quitCh := make(chan struct{})
	return &RequestHandler{
		FullExporter: fullExporter,
		Inquirer:     &MongoInquirer{Mongo: mongo},
		Locker: Locker{
			locked: lockedCh,
			acked:  ackedCh,
			quit:   quitCh,
		},
	}
}

func (handler *RequestHandler) Export(writer http.ResponseWriter, request *http.Request) {
	defer request.Body.Close()

	tid := transactionidutils.GetTransactionIDFromRequest(request)

	handler.Locker.locked <- true

	select {
	case <-handler.Locker.acked:
		log.Info("Locker acquired")
		defer func() {
			log.Info("Locker released")
			handler.locked <- false
		}()
	case <-time.After(time.Second * 3):
		msg := "Stopping kafka consumption timed out"
		log.Infof(msg)
		http.Error(writer, msg, http.StatusServiceUnavailable)
		return
	}
	jobID := uuid.New()
	job := &Job{ID: jobID, NrWorker: handler.FullExporter.nrOfConcurrentWorkers, Status: STARTING}
	handler.FullExporter.AddJob(job)

	go func() {
		log.Infoln("Calling mongo")
		docs, err, count := handler.Inquirer.Inquire("content")
		if err != nil {
			msg := fmt.Sprintf(`Failed to read IDs from mongo for %v! "%v"`, "content", err.Error())
			log.Info(msg)
			job.ErrorMessage = msg
			job.Status = FINISHED
			return
		}
		log.Infof("Nr of UUIDs found: %v", count)
		job.DocIds = docs
		job.Count = count

		job.RunFullExport(tid, handler.FullExporter.HandleContent)
	}()

	writer.WriteHeader(http.StatusAccepted)
	writer.Header().Add("Content-Type", "application/json")

	err := json.NewEncoder(writer).Encode(job)
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

	job, err := handler.FullExporter.GetJob(jobID)
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

func (handler *RequestHandler) GetRunningJobs(writer http.ResponseWriter, request *http.Request) {
	defer request.Body.Close()

	writer.Header().Add("Content-Type", "application/json")

	jobs := handler.FullExporter.GetRunningJobs()

	err := json.NewEncoder(writer).Encode(jobs)
	if err != nil {
		msg := fmt.Sprintf(`Failed to get running jobs: "%v"`, err)
		log.Warn(msg)
		fmt.Fprintf(writer, "{\"Jobs\": \"%v\"}", jobs)
		return
	}
}
