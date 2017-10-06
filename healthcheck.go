package main

import (
	"github.com/Financial-Times/content-exporter/content"
	health "github.com/Financial-Times/go-fthealth/v1_1"
	"github.com/Financial-Times/service-status-go/gtg"
	"github.com/Financial-Times/content-exporter/db"
	"github.com/Financial-Times/content-exporter/queue"
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
	svc := &healthService{config: config}
	svc.checks = []health.Check{
		svc.MongoCheck(),
		svc.ReadEndpointCheck(),
		svc.S3WriterCheck(),
		svc.KafkaCheck(),
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

func (service *healthService) ReadEndpointCheck() health.Check {
	return health.Check{
		Name:             "CheckConnectivityToApiPolicyComponent",
		BusinessImpact:   "No Business Impact.",
		PanicGuide:       "https://dewey.ft.com/content-exporter.html",
		Severity:         2,
		TechnicalSummary: "The service is unable to connect to Api Policy Component. Neither FULL nor INCREMENTAL or TARGETED export won't work because of this",
		Checker:          service.config.enrichedContentFetcher.CheckHealth,
	}
}

func (service *healthService) S3WriterCheck() health.Check {
	return health.Check{
		Name:             "CheckConnectivityToContentRWS3",
		BusinessImpact:   "No Business Impact.",
		PanicGuide:       "https://dewey.ft.com/content-exporter.html",
		Severity:         2,
		TechnicalSummary: "The service is unable to connect to Content-RW-S3. Neither FULL nor INCREMENTAL or TARGETED export won't work because of this",
		Checker:          service.config.s3Uploader.CheckHealth,
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
