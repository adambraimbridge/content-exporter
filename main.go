package main

import (
	log "github.com/Sirupsen/logrus"
	"github.com/jawher/mow.cli"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"

	"github.com/Financial-Times/http-handlers-go/httphandlers"
	"github.com/gorilla/mux"
	"github.com/rcrowley/go-metrics"

	"github.com/Financial-Times/content-exporter/service"
	health "github.com/Financial-Times/go-fthealth/v1_1"
	status "github.com/Financial-Times/service-status-go/httphandlers"
	"errors"
	"strings"
	"net"
	"fmt"
)

const appDescription = "Exports content from DB and sends to S3"

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
		mongo := service.NewMongoDatabase(*mongos, 10)
		go func() {
			serveEndpoints(*appSystemCode, *appName, *port, requestHandler{&service.MongoInquirer{mongo}})
		}()

		waitForSignal()
	}
	err := app.Run(os.Args)
	if err != nil {
		log.Errorf("App could not start, error=[%s]\n", err)
		return
	}
}

func serveEndpoints(appSystemCode string, appName string, port string, requestHandler requestHandler) {
	healthService := newHealthService(&healthConfig{appSystemCode: appSystemCode, appName: appName, port: port})

	serveMux := http.NewServeMux()

	hc := health.HealthCheck{SystemCode: appSystemCode, Name: appName, Description: appDescription, Checks: healthService.checks}
	serveMux.HandleFunc(healthPath, health.Handler(hc))
	serveMux.HandleFunc(status.GTGPath, status.NewGoodToGoHandler(healthService.gtgCheck))
	serveMux.HandleFunc(status.BuildInfoPath, status.BuildInfoHandler)

	servicesRouter := mux.NewRouter()
	servicesRouter.HandleFunc("/export", requestHandler.export).Methods("GET")
	//todo: add new handlers here

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
		host, port, err := net.SplitHostPort(mongoUrl);
		if err != nil {
			return fmt.Errorf("Cannot split MongoDB URL: %s into host and port. Error is: %s",mongoUrl, err.Error())
		}
		if host == "" || port == "" {
			return fmt.Errorf("One of the MongoDB URLs is incomplete: %s. It should have host and port.", mongoUrl)
		}
	}

	return nil
}