package main

import (
	"fmt"
	"github.com/Financial-Times/content-exporter/content"
	log "github.com/sirupsen/logrus"
	"sync"
)

type Content struct {
	Uuid, Date string
}

type ContentExporter struct {
	Fetcher  content.Fetcher
	Uploader content.Uploader
}

func (e *ContentExporter) HandleContent(tid string, doc Content) error {
	payload, err := e.Fetcher.GetContent(doc.Uuid, tid)
	if err != nil {
		log.Errorf("Error by getting content for %v: %v\n", doc.Uuid, err)
		return err
	}

	err = e.Uploader.Upload(payload, tid, doc.Uuid, doc.Date)
	if err != nil {
		log.Errorf("Error by uploading content for %v: %v\n", doc.Uuid, err)
		return err
	}
	return nil
}

type Pool interface {
	AddJob(job *Job)
	GetJob(jobID string) (Job, error)
	GetRunningJobs() []Job
}

type JobPool struct {
	sync.RWMutex
	jobs                  map[string]*Job
	NrOfConcurrentWorkers int
}

type State string

const (
	RUNNING  State = "Running"
	FINISHED State = "Finished"
)

//TODO use this to runExport handling
type Runner func()

type Job struct {
	sync.RWMutex
	wg       sync.WaitGroup
	NrWorker int          `json:"-"`
	DocIds   chan Content `json:"-"`
	ID       string       `json:"ID"`
	Count    int          `json:"ApproximateCount,omitempty"`
	Progress int          `json:"Progress,omitempty"`
	Failed   []string     `json:"Failed,omitempty"`
	Status   State        `json:"Status"`
}

func NewJobPool(nrOfWorkers int) *JobPool {
	return &JobPool{
		jobs: make(map[string]*Job),
		NrOfConcurrentWorkers: nrOfWorkers,
	}
}

func (p *JobPool) GetRunningJobs() []Job {
	p.RLock()
	defer p.RUnlock()
	var jobs []Job
	for _, job := range p.jobs {
		if job.Status == RUNNING {
			jobs = append(jobs, job.Copy())
		}
	}
	return jobs
}

func (p *JobPool) GetJob(jobID string) (Job, error) {
	p.RLock()
	defer p.RUnlock()
	job, ok := p.jobs[jobID]
	if !ok {
		return Job{}, fmt.Errorf("Job %v not found", jobID)
	}
	return job.Copy(), nil
}

func (p *JobPool) AddJob(job *Job) {
	if job != nil {
		p.Lock()
		p.jobs[job.ID] = job
		p.Unlock()
	}
}

func (job *Job) Copy() Job {
	job.Lock()
	defer job.Unlock()
	return Job{
		Progress: job.Progress,
		Status:   job.Status,
		ID:       job.ID,
		Count:    job.Count,
		Failed:   job.Failed,
	}
}

func (job *Job) RunFullExport(tid string, export func(string, Content) error) {
	log.Infof("Job started: %v", job.ID)
	worker := make(chan struct{}, job.NrWorker)
	for {
		doc, ok := <-job.DocIds
		if !ok {
			job.wg.Wait()
			job.Status = FINISHED
			log.Infof("Finished job %v with %v failure(s), progress: %v", job.ID, len(job.Failed), job.Progress)
			close(worker)
			return
		}

		worker <- struct{}{} // Will block until worker is available to span up new goroutines

		job.Progress++
		job.wg.Add(1)
		go func() {
			defer job.wg.Done()
			job.runExport(export, tid, doc)
			<-worker
		}()
	}
}

func (job *Job) runExport(export func(string, Content) error, tid string, doc Content) {
	if err := export(tid, doc); err != nil {
		job.Lock()
		job.Failed = append(job.Failed, doc.Uuid)
		job.Unlock()
	}
}
