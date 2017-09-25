package main

import (
	"github.com/jawher/mow.cli"
	log "github.com/sirupsen/logrus"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"

	"github.com/Financial-Times/http-handlers-go/httphandlers"
	"github.com/gorilla/mux"
	"github.com/rcrowley/go-metrics"

	"errors"
	"fmt"
	"github.com/Financial-Times/content-exporter/content"
	"github.com/Financial-Times/content-exporter/db"
	apphttp "github.com/Financial-Times/content-exporter/http"
	"github.com/Financial-Times/content-exporter/service"
	health "github.com/Financial-Times/go-fthealth/v1_1"
	status "github.com/Financial-Times/service-status-go/httphandlers"
	"github.com/sethgrid/pester"
	"net"
	"strings"
	"time"
)

const appDescription = "Exports content from DB and sends to S3"

func init() {
	f := &log.JSONFormatter{
		TimestampFormat: time.RFC3339Nano,
	}

	log.SetFormatter(f)
}

func main() {
	app := cli.App("content-exporter", appDescription)

	appSystemCode := app.String(cli.StringOpt{
		Name:   "app-system-code",
		Value:  "content-exporter",
		Desc:   "System Code of the application",
		EnvVar: "APP_SYSTEM_CODE",
	})
	appName := app.String(cli.StringOpt{
		Name:   "app-name",
		Value:  "content-exporter",
		Desc:   "Application name",
		EnvVar: "APP_NAME",
	})
	port := app.String(cli.StringOpt{
		Name:   "port",
		Value:  "8080",
		Desc:   "Port to listen on",
		EnvVar: "APP_PORT",
	})
	mongos := app.String(cli.StringOpt{
		Name:   "mongoConnection",
		Value:  "",
		Desc:   "Mongo addresses to connect to in format: host1[:port1][,host2[:port2],...]",
		EnvVar: "MONGO_CONNECTION",
	})
	enrichedContentBaseURL := app.String(cli.StringOpt{
		Name:   "enrichedContentBaseURL",
		Value:  "http://localhost:8080",
		Desc:   "Base URL to enriched content endpoint",
		EnvVar: "ENRICHED_CONTENT_BASE_URL",
	})
	enrichedContentHealthURL := app.String(cli.StringOpt{
		Name:   "enrichedContentHealthURL",
		Value:  "http://localhost:8080/__gtg",
		Desc:   "Health URL to enriched content endpoint",
		EnvVar: "ENRICHED_CONTENT_HEALTH_URL",
	})
	s3WriterBaseURL := app.String(cli.StringOpt{
		Name:   "s3WriterBaseURL",
		Value:  "http://localhost:8080",
		Desc:   "Base URL to S3 writer endpoint",
		EnvVar: "S3_WRITER_BASE_URL",
	})
	s3WriterHealthURL := app.String(cli.StringOpt{
		Name:   "s3WriterHealthURL",
		Value:  "http://localhost:8080/__gtg",
		Desc:   "Base URL to S3 writer endpoint",
		EnvVar: "S3_WRITER_HEALTH_URL",
	})
	xPolicyHeaderValues := app.String(cli.StringOpt{
		Name:   "xPolicyHeaderValues",
		Value:  "",
		Desc:   "Values for X-Policy header separated by comma, e.g. INCLUDE_RICH_CONTENT,EXPAND_IMAGES",
		EnvVar: "X_POLICY_HEADER_VALUES",
	})
	authorization := app.String(cli.StringOpt{
		Name:   "authorization",
		Value:  "",
		Desc:   "Authorization for enrichedcontent endpoint",
		EnvVar: "AUTHORIZATION",
	})

	log.SetLevel(log.InfoLevel)
	log.Infof("[Startup] content-exporter is starting ")

	app.Before = func() {
		if err := checkMongoURLs(*mongos); err != nil {
			app.PrintHelp()
			log.Fatalf("Mongo connection is not set correctly: %s", err.Error())
		}
	}

	app.Action = func() {
		log.Infof("System code: %s, App Name: %s, Port: %s, Mongo connection: %s", *appSystemCode, *appName, *port, *mongos)
		mongo := db.NewMongoDatabase(*mongos, 100)

		tr := &http.Transport{
			MaxIdleConnsPerHost: 128,
			Dial: (&net.Dialer{
				Timeout:   30 * time.Second,
				KeepAlive: 30 * time.Second,
			}).Dial,
		}
		c := &http.Client{
			Transport: tr,
			Timeout:   30 * time.Second,
		}
		client := pester.NewExtendedClient(c)
		client.Backoff = pester.ExponentialBackoff
		client.MaxRetries = 3
		client.Concurrency = 1

		go func() {
			fetcher := &content.EnrichedContentFetcher{Client: client,
				EnrichedContentBaseURL:   *enrichedContentBaseURL,
				EnrichedContentHealthURL: *enrichedContentHealthURL,
				XPolicyHeaderValues:      *xPolicyHeaderValues,
				Authorization:            *authorization,
			}
			uploader := &content.S3Uploader{Client: client, S3WriterBaseURL: *s3WriterBaseURL, S3WriterHealthURL: *s3WriterHealthURL}
			serveEndpoints(*appSystemCode, *appName, *port, apphttp.RequestHandler{
				JobPool:  service.NewJobPool(30),
				Inquirer: &db.MongoInquirer{Mongo: mongo},
				ContentExporter: &service.ContentExporter{
					Fetcher:  fetcher,
					Uploader: uploader,
				},
			}, mongo, fetcher, uploader)
		}()

		waitForSignal()
	}
	err := app.Run(os.Args)
	if err != nil {
		log.Errorf("App could not start, error=[%s]\n", err)
		return
	}
}

func serveEndpoints(appSystemCode string, appName string, port string, requestHandler apphttp.RequestHandler,
	mongo db.DB, fetcher *content.EnrichedContentFetcher, uploader *content.S3Uploader) {
	healthService := newHealthService(&healthConfig{appSystemCode: appSystemCode, appName: appName, port: port, db: mongo, enrichedContentFetcher: fetcher, s3Uploader: uploader})

	serveMux := http.NewServeMux()

	hc := health.HealthCheck{SystemCode: appSystemCode, Name: appName, Description: appDescription, Checks: healthService.checks}
	serveMux.HandleFunc(healthPath, health.Handler(hc))
	serveMux.HandleFunc(status.GTGPath, status.NewGoodToGoHandler(healthService.gtgCheck))
	serveMux.HandleFunc(status.BuildInfoPath, status.BuildInfoHandler)

	servicesRouter := mux.NewRouter()
	servicesRouter.HandleFunc("/export", requestHandler.Export).Methods(http.MethodPost)
	servicesRouter.HandleFunc("/jobs/{jobID}", requestHandler.GetJob).Methods(http.MethodGet)
	servicesRouter.HandleFunc("/jobs", requestHandler.GetRunningJobs).Methods(http.MethodGet)

	var monitoringRouter http.Handler = servicesRouter
	monitoringRouter = httphandlers.TransactionAwareRequestLoggingHandler(log.StandardLogger(), monitoringRouter)
	monitoringRouter = httphandlers.HTTPMetricsHandler(metrics.DefaultRegistry, monitoringRouter)

	serveMux.Handle("/", monitoringRouter)

	server := &http.Server{Addr: ":" + port, Handler: serveMux}

	wg := sync.WaitGroup{}

	wg.Add(1)
	go func() {
		if err := server.ListenAndServe(); err != nil {
			log.Infof("HTTP server closing with message: %v", err)
		}
		wg.Done()
	}()

	waitForSignal()
	log.Infof("[Shutdown] content-exporter is shutting down")

	if err := server.Close(); err != nil {
		log.Errorf("Unable to stop http server: %v", err)
	}

	wg.Wait()
}

func waitForSignal() {
	ch := make(chan os.Signal)
	signal.Notify(ch, syscall.SIGINT, syscall.SIGTERM)
	<-ch
}

func checkMongoURLs(providedMongoUrls string) error {
	if providedMongoUrls == "" {
		return errors.New("MongoDB urls are missing")
	}

	mongoUrls := strings.Split(providedMongoUrls, ",")

	for _, mongoUrl := range mongoUrls {
		host, port, err := net.SplitHostPort(mongoUrl)
		if err != nil {
			return fmt.Errorf("Cannot split MongoDB URL: %s into host and port. Error is: %s", mongoUrl, err.Error())
		}
		if host == "" || port == "" {
			return fmt.Errorf("One of the MongoDB URLs is incomplete: %s. It should have host and port.", mongoUrl)
		}
	}

	return nil
}
