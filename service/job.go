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
	wg sync.WaitGroup
	sync.RWMutex
	nrWorker int
	ID       string          `json:"ID"`
	DocIds   chan db.Content `json:"-"`
	Count    int             `json:"Count,omitempty"`
	Progress int             `json:"Progress,omitempty"`
	Failed   []string        `json:"Failed,omitempty"`
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

func (job *job) Run(handler *RequestHandler, tid string, export func(string, db.Content) error) {
	log.Infof("Job started: %v", job.ID)
	worker := make(chan struct{}, job.nrWorker)
	for {
		doc, ok := <-job.DocIds
		if !ok {
			job.wg.Wait()
			log.Infof("Finished job %v with %v failure(s), progress: %v", job.ID, len(job.Failed), job.Progress)
			return
		}
		job.Lock()
		job.Progress++
		job.Unlock()
		worker <- struct{}{}  // Wait until worker is available to span up new goroutines
		job.wg.Add(1)
		go job.submit(export, tid, doc, worker)

	}
}

func (job *job) submit(export func(string, db.Content) error, tid string, doc db.Content, worker chan struct{}) {
	defer job.wg.Done()
	if err := export(tid, doc); err != nil {
		job.Lock()
		job.Failed = append(job.Failed, doc.Uuid)
		job.Unlock()
	}
	<- worker
}
