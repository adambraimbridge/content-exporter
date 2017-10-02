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

type FullExporter struct {
	sync.RWMutex
	jobs                  map[string]*Job
	nrOfConcurrentWorkers int
	*ContentExporter
}

type State string

const (
	STARTING State = "Starting"
	RUNNING  State = "Running"
	FINISHED State = "Finished"
)

type Job struct {
	sync.RWMutex
	wg           sync.WaitGroup
	NrWorker     int          `json:"-"`
	DocIds       chan Content `json:"-"`
	ID           string       `json:"ID"`
	Count        int          `json:"ApproximateCount,omitempty"`
	Progress     int          `json:"Progress,omitempty"`
	Failed       []string     `json:"Failed,omitempty"`
	Status       State        `json:"Status"`
	ErrorMessage string       `json:"ErrorMessage,omitempty"`
}

func NewFullExporter(nrOfWorkers int, exporter *ContentExporter) *FullExporter {
	return &FullExporter{
		jobs: make(map[string]*Job),
		nrOfConcurrentWorkers: nrOfWorkers,
		ContentExporter:       exporter,
	}
}

func (fe *FullExporter) GetRunningJobs() []Job {
	fe.RLock()
	defer fe.RUnlock()
	var jobs []Job
	for _, job := range fe.jobs {
		if job.Status == RUNNING {
			jobs = append(jobs, job.Copy())
		}
	}
	return jobs
}

func (fe *FullExporter) GetJob(jobID string) (Job, error) {
	fe.RLock()
	defer fe.RUnlock()
	job, ok := fe.jobs[jobID]
	if !ok {
		return Job{}, fmt.Errorf("Job %v not found", jobID)
	}
	return job.Copy(), nil
}

func (fe *FullExporter) AddJob(job *Job) {
	if job != nil {
		fe.Lock()
		fe.jobs[job.ID] = job
		fe.Unlock()
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
	job.Status = RUNNING
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
			if err := export(tid, doc); err != nil {
				job.Lock()
				job.Failed = append(job.Failed, doc.Uuid)
				job.Unlock()
			}
			<-worker
		}()
	}
}
