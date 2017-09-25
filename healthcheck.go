package main

import (
	"context"
	"github.com/Financial-Times/content-exporter/content"
	"github.com/Financial-Times/content-exporter/db"
	health "github.com/Financial-Times/go-fthealth/v1_1"
	"github.com/Financial-Times/service-status-go/gtg"
	"time"
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
	db                     db.DB
	enrichedContentFetcher *content.EnrichedContentFetcher
	s3Uploader             *content.S3Uploader
}

func newHealthService(config *healthConfig) *healthService {
	service := &healthService{config: config}
	service.checks = []health.Check{
		service.DBCheck(config.db),
		service.ReadEndpointCheck(),
		service.S3WriterCheck(),
	}
	return service
}

func (service *healthService) DBCheck(db db.DB) health.Check {
	return health.Check{
		Name:             "CheckConnectivityToMongoDatabase",
		BusinessImpact:   "No Business Impact.",
		PanicGuide:       "https://dewey.ft.com/content-exporter.html",
		Severity:         1,
		TechnicalSummary: "The service is unable to connect to MongoDB. Full export won't work because of this",
		Checker:          service.pingMongo(db),
	}
}

func (service *healthService) ReadEndpointCheck() health.Check {
	return health.Check{
		Name:             "CheckConnectivityToApiPolicyComponent",
		BusinessImpact:   "No Business Impact.",
		PanicGuide:       "https://dewey.ft.com/content-exporter.html",
		Severity:         1,
		TechnicalSummary: "The service is unable to connect to Api Policy Component. Neither Full nor Incremental Export won't work because of this",
		Checker:          service.config.enrichedContentFetcher.CheckHealth,
	}
}

func (service *healthService) S3WriterCheck() health.Check {
	return health.Check{
		Name:             "CheckConnectivityToContentRWS3",
		BusinessImpact:   "No Business Impact.",
		PanicGuide:       "https://dewey.ft.com/content-exporter.html",
		Severity:         1,
		TechnicalSummary: "The service is unable to connect to Content-RW-S3. Neither Full nor Incremental Export won't work because of this",
		Checker:          service.config.s3Uploader.CheckHealth,
	}                                  	
}

func (service *healthService) pingMongo(db db.DB) func() (string, error) {
	return func() (string, error) {
		tx, err := db.Open()
		if err != nil {
			return "", err
		}

		defer func() { go tx.Close() }()

		ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
		defer cancel()

		err = tx.Ping(ctx)
		if err != nil {
			return "", err
		}

		return "OK", nil
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
