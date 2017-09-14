package service

import (
	"fmt"
	"github.com/Financial-Times/content-exporter/db"
	log "github.com/sirupsen/logrus"
	"sync"
)

type Pool interface {
	AddJob(job *job)
	GetJob(jobID string) (*job, error)
}

type JobPool struct {
	sync.RWMutex
	jobs map[string]*job
}

type job struct {
	ID       string            `json:"ID"`
	docIds   chan db.DBContent `json:"-"`
	Count    int               `json:"Count,omitempty"`
	Progress int               `json:"Progress,omitempty"`
	Failed   []string          `json:"Failed,omitempty"`
}

func NewJobPool() *JobPool {
	return &JobPool{
		jobs: make(map[string]*job),
	}
}

func (p *JobPool) GetJob(jobID string) (*job, error) {
	p.RLock()
	defer p.RUnlock()
	job, ok := p.jobs[jobID]
	if !ok {
		return nil, fmt.Errorf("Job %v not found", jobID)
	}
	return job, nil
}

func (p *JobPool) AddJob(job *job) {
	if job != nil {
		p.Lock()
		defer p.Unlock()
		p.jobs[job.ID] = job
	}
}

func (job *job) Run(handler *RequestHandler, tid string) {
	for {
		doc, ok := <-job.docIds
		if !ok {
			log.Infof("Finished job %v with %v failures", job.ID, len(job.Failed))
			return
		}

		payload, err := handler.Exporter.GetContent(doc.Uuid, tid)
		if err != nil {
			job.Failed = append(job.Failed, doc.Uuid)
			log.Errorf("Error by getting content for %v: %v\n", doc.Uuid, err)
			continue
		}

		err = handler.Uploader.Upload(payload, tid, doc.Uuid, doc.Date)
		if err != nil {
			job.Failed = append(job.Failed, doc.Uuid)
			log.Errorf("Error by uploading content for %v: %v\n", doc.Uuid, err)
			continue
		}
		job.Progress++
	}
}
