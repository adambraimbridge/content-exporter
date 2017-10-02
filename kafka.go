package main

import (
	"encoding/json"
	"fmt"
	"github.com/Financial-Times/kafka-client-go/kafka"
	log "github.com/sirupsen/logrus"
	"regexp"
	"strings"
	"sync"
	"time"
)

type eventType string

const UPDATE eventType = "UPDATE"
const DELETE eventType = "DELETE"

type MessageQueueHandler interface {
	HandleMessage(queueMsg kafka.FTMessage) error
}

type KafkaMessageHandler struct {
	messageConsumer kafka.Consumer
	ContentExporter *ContentExporter
	Delay           int
	WhiteListRegex  *regexp.Regexp
	*Locker
	sync.RWMutex
	running bool
}

func NewKafkaMessageHandler(exporter *ContentExporter, delayForNotification int, messageConsumer kafka.Consumer, whitelistR *regexp.Regexp, locker *Locker) *KafkaMessageHandler {
	return &KafkaMessageHandler{
		ContentExporter: exporter,
		Delay:           delayForNotification,
		messageConsumer: messageConsumer,
		WhiteListRegex:  whitelistR,
		Locker:          locker,
	}
}

type NotificationQueueMessage struct {
	kafka.FTMessage
}

type PublicationEvent struct {
	ContentURI   string
	UUID         string
	Payload      interface{}
	LastModified string
}

func (e PublicationEvent) HasEmptyPayload() bool {
	switch v := e.Payload.(type) {
	case nil:
		return true
	case string:
		if len(v) == 0 {
			return true
		}
	case map[string]interface{}:
		if len(v) == 0 {
			return true
		}
	}
	return false
}

func (e PublicationEvent) Matches(whiteList *regexp.Regexp) bool {
	return whiteList.MatchString(e.ContentURI)
}

func (msg NotificationQueueMessage) ToPublicationEvent() (event PublicationEvent, err error) {
	err = json.Unmarshal([]byte(msg.Body), &event)
	return event, err
}

func (msg NotificationQueueMessage) HasSynthTransactionID() bool {
	tid := msg.TransactionID()
	return strings.HasPrefix(tid, "SYNTH")
}

func (msg NotificationQueueMessage) TransactionID() string {
	return msg.Headers["X-Request-Id"]
}

func (h *KafkaMessageHandler) startConsuming() {
	h.Lock()
	defer h.Unlock()
	if !h.running {
		h.running = true
		h.messageConsumer.StartListening(h.handleMessage)
	}
}

func (h *KafkaMessageHandler) stopConsuming() {
	h.Lock()
	defer h.Unlock()
	if h.running {
		h.running = false
		h.messageConsumer.Shutdown()
	}
}

func (h *KafkaMessageHandler) ConsumeMessages() {
	h.startConsuming()
	defer h.stopConsuming()
	for {
		select {
		case locked := <-h.Locker.locked:
			log.Infof("LOCK signal received: %v...", locked)
			if locked {
				h.stopConsuming()
			} else {
				h.startConsuming()
			}
			select {
			case h.Locker.acked <- struct{}{}:
				log.Infof("LOCK acked")
				case <-time.After(time.Second * 2):
			}
		case <-h.quit:
			log.Infof("QUIT signal received...")
			return
		}
	}
}

func (h *KafkaMessageHandler) StopConsumingMessages() {
	h.quit <- struct{}{}
	for {
		if !h.running {
			return
		}
		time.Sleep(time.Millisecond * 100)
	}
}

func (h *KafkaMessageHandler) handleMessage(queueMsg kafka.FTMessage) error {
	msg := NotificationQueueMessage{queueMsg}

	pubEvent, err := msg.ToPublicationEvent()
	tid := msg.TransactionID()
	if err != nil {
		log.WithField("transaction_id", tid).WithField("msg", msg.Body).WithError(err).Warn("Skipping event.")
		return err
	}

	if msg.HasSynthTransactionID() {
		log.WithField("transaction_id", tid).WithField("contentUri", pubEvent.ContentURI).Info("Skipping event: Synthetic transaction ID.")
		return nil
	}

	if !pubEvent.Matches(h.WhiteListRegex) {
		log.WithField("transaction_id", tid).WithField("contentUri", pubEvent.ContentURI).Info("Skipping event: It is not in the whitelist.")
		return nil
	}

	doc, evType, err := h.MapNotification(pubEvent)
	if err != nil {
		log.WithField("transaction_id", tid).WithField("msg", msg.Body).WithError(err).Warn("Skipping event: Cannot build notification for message.")
		return err
	}

	if evType == UPDATE {
		time.Sleep(time.Duration(h.Delay) * time.Second)
		log.WithField("transaction_id", tid).WithField("uuid", doc.Uuid).Info("UPDATE event received")
		h.ContentExporter.HandleContent(tid, doc)
	} else if evType == DELETE {
		//TODO handle delete event
		log.WithField("transaction_id", tid).WithField("uuid", doc.Uuid).Info("DELETE event received")
		//h.ContentExporter.Uploader.Delete(doc.Uuid)
	}
	return nil
}

func (h *KafkaMessageHandler) CheckHealth() (string, error) {
	if err := h.messageConsumer.ConnectivityCheck(); err != nil {
		return "Kafka is not good to go.", err
	}
	return "Kafka is good to go.", nil
}

// UUIDRegexp enables to check if a string matches a UUID
var UUIDRegexp = regexp.MustCompile("[a-fA-F0-9]{8}-[a-fA-F0-9]{4}-[a-fA-F0-9]{4}-[a-fA-F0-9]{4}-[a-fA-F0-9]{12}")

// MapNotification maps the given event to a new notification.
func (h *KafkaMessageHandler) MapNotification(event PublicationEvent) (Content, eventType, error) {
	UUID := UUIDRegexp.FindString(event.ContentURI)
	if UUID == "" {
		return Content{}, eventType(""), fmt.Errorf("ContentURI does not contain a UUID")
	}

	var evType eventType
	var date = defaultDate

	if event.HasEmptyPayload() {
		evType = DELETE
	} else {
		evType = UPDATE
		notificationPayloadMap, ok := event.Payload.(map[string]interface{})
		if ok {
			date = getDate(notificationPayloadMap)
		}
	}

	return Content{
		Uuid: UUID,
		Date: date,
	}, evType, nil
}
