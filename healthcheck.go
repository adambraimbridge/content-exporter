package main

import (
	"github.com/Financial-Times/content-exporter/content"
	"github.com/Financial-Times/content-exporter/db"
	"github.com/Financial-Times/content-exporter/queue"
	health "github.com/Financial-Times/go-fthealth/v1_1"
	"github.com/Financial-Times/service-status-go/gtg"
	"net/http"
	"time"
	"net"
)

const healthPath = "/__health"

type healthService struct {
	config *healthConfig
	checks []health.Check
}

type healthConfig struct {
	appSystemCode          string
	appName                string
	port                   string
	db                     *db.MongoDB
	enrichedContentFetcher *content.EnrichedContentFetcher
	s3Uploader             *content.S3Updater
	queueHandler           *queue.KafkaListener
}

func newHealthService(config *healthConfig) *healthService {
	tr := &http.Transport{
		MaxIdleConnsPerHost: 10,
		Dial: (&net.Dialer{
			Timeout:   3 * time.Second,
			KeepAlive: 3 * time.Second,
		}).Dial,
	}
	httpClient := &http.Client{
		Transport: tr,
		Timeout:   3 * time.Second,
	}
	svc := &healthService{config: config}
	svc.checks = []health.Check{
		svc.MongoCheck(),
		svc.ReadEndpointCheck(httpClient),
		svc.S3WriterCheck(httpClient),
	}
	if config.queueHandler != nil {
		svc.checks = append(svc.checks, svc.KafkaCheck())
	}
	return svc
}

func (service *healthService) MongoCheck() health.Check {
	return health.Check{
		Name:             "CheckConnectivityToMongoDatabase",
		BusinessImpact:   "No Business Impact.",
		PanicGuide:       "https://dewey.ft.com/content-exporter.html",
		Severity:         2,
		TechnicalSummary: "The service is unable to connect to MongoDB. FULL or TARGETED export won't work because of this",
		Checker:          service.config.db.CheckHealth,
	}
}

func (service *healthService) ReadEndpointCheck(client content.Client) health.Check {
	return health.Check{
		Name:             "CheckConnectivityToApiPolicyComponent",
		BusinessImpact:   "No Business Impact.",
		PanicGuide:       "https://dewey.ft.com/content-exporter.html",
		Severity:         2,
		TechnicalSummary: "The service is unable to connect to Api Policy Component. Neither FULL nor INCREMENTAL or TARGETED export won't work because of this",
		Checker: func() (string, error) {
			return service.config.enrichedContentFetcher.CheckHealth(client)
		},
	}
}

func (service *healthService) S3WriterCheck(client content.Client) health.Check {
	return health.Check{
		Name:             "CheckConnectivityToContentRWS3",
		BusinessImpact:   "No Business Impact.",
		PanicGuide:       "https://dewey.ft.com/content-exporter.html",
		Severity:         2,
		TechnicalSummary: "The service is unable to connect to Content-RW-S3. Neither FULL nor INCREMENTAL or TARGETED export won't work because of this",
		Checker: func() (string, error) {
			return service.config.s3Uploader.CheckHealth(client)
		},
	}
}

func (service *healthService) KafkaCheck() health.Check {
	return health.Check{
		Name:             "CheckConnectivityToKafka",
		BusinessImpact:   "No Business Impact.",
		PanicGuide:       "https://dewey.ft.com/content-exporter.html",
		Severity:         2,
		TechnicalSummary: "The service is unable to connect to Kafka. INCREMENTAL export won't work because of this",
		Checker:          service.config.queueHandler.CheckHealth,
	}
}

func (service *healthService) gtgCheck() gtg.Status {
	for _, check := range service.checks {
		if _, err := check.Checker(); err != nil {
			return gtg.Status{GoodToGo: false, Message: err.Error()}
		}
	}
	return gtg.Status{GoodToGo: true}
}
