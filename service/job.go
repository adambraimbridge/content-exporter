package service

import (
	"fmt"
	"github.com/Financial-Times/content-exporter/db"
	log "github.com/sirupsen/logrus"
	"sync"
)

//type Pool interface {
//	AddJob(job *job)
//	GetJob(jobID string) (*job, error)
//	GetRunningJobs() []*job
//}

type JobPool struct {
	sync.RWMutex
	jobs map[string]*job
	NrOfConcurrentWorkers int
}

type State string

const (
	RUNNING State = "Running"
	FINISHED State = "Finished"
)

type job struct {
	sync.RWMutex
	wg       sync.WaitGroup
	nrWorker int
	docIds   chan db.Content
	ID       string          `json:"ID"`
	Count    int             `json:"ApproximateCount,omitempty"`
	Progress int             `json:"Progress,omitempty"`
	Failed   []string        `json:"Failed,omitempty"`
	Status   State      `json:"Status"`
}

func NewJobPool(nrOfWorkers int) *JobPool {
	return &JobPool{
		jobs: make(map[string]*job),
		NrOfConcurrentWorkers: nrOfWorkers,
	}
}

func (p *JobPool) GetRunningJobs() []*job {
	p.RLock()
	defer p.RUnlock()
	var jobs []*job
	for _, job := range p.jobs {
		if job.Status == RUNNING {
			jobs = append(jobs, job)
		}
	}
	return jobs
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
	fmt.Printf("DEBUG INFO - ")
	for {
		doc, ok := <-job.docIds
		if !ok {
			fmt.Printf("\n")
			job.wg.Wait()
			job.Status = FINISHED
			log.Infof("Finished job %v with %v failure(s), progress: %v", job.ID, len(job.Failed), job.Progress)
			return
		}
		job.Lock()
		job.Progress++
		fmt.Printf(" %v", job.Progress)
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
