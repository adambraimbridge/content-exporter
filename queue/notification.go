package queue

import (
	"fmt"
	"github.com/Financial-Times/content-exporter/content"
	"github.com/Financial-Times/content-exporter/export"
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
	"regexp"
	"time"
)

type EventType string

const UPDATE EventType = "UPDATE"
const DELETE EventType = "DELETE"

type Notification struct {
	Stub   content.Stub
	EvType EventType
	Tid    string
	*export.Terminator
}

// UUIDRegexp enables to check if a string matches a UUID
var UUIDRegexp = regexp.MustCompile("[a-fA-F0-9]{8}-[a-fA-F0-9]{4}-[a-fA-F0-9]{4}-[a-fA-F0-9]{4}-[a-fA-F0-9]{12}")

type KafkaMessageHandler struct {
	WhiteListRegex  *regexp.Regexp
	ContentExporter *content.Exporter
	Delay           int
}

func NewKafkaMessageHandler(exporter *content.Exporter, delayForNotification int, whitelistR *regexp.Regexp) *KafkaMessageHandler {
	return &KafkaMessageHandler{
		ContentExporter: exporter,
		Delay:           delayForNotification,
		WhiteListRegex:  whitelistR,
	}
}

func (h *KafkaMessageHandler) handleMessage(msg Message, tid string) (*Notification, error) {
	pubEvent, err := msg.ToPublicationEvent()
	if err != nil {
		log.WithField("transaction_id", tid).WithField("msg", msg.Body).WithError(err).Warn("Skipping event.")
		return &Notification{}, err
	}

	if msg.HasSynthTransactionID() {
		log.WithField("transaction_id", tid).WithField("contentUri", pubEvent.ContentURI).Info("Skipping event: Synthetic transaction ID.")
		return &Notification{}, nil
	}

	if !pubEvent.Matches(h.WhiteListRegex) {
		log.WithField("transaction_id", tid).WithField("contentUri", pubEvent.ContentURI).Info("Skipping event: It is not in the whitelist.")
		return &Notification{}, nil
	}

	n, err := pubEvent.MapNotification()
	if err != nil {
		log.WithField("transaction_id", tid).WithField("msg", msg.Body).WithError(err).Warn("Skipping event: Cannot build notification for message.")
		return &Notification{}, err
	}
	n.Tid = tid

	return n, nil
}

func (e PublicationEvent) MapNotification() (*Notification, error) {
	UUID := UUIDRegexp.FindString(e.ContentURI)
	if UUID == "" {
		return &Notification{Stub: content.Stub{}, EvType: EventType("")}, fmt.Errorf("ContentURI does not contain a UUID")
	}

	var evType EventType
	var date = content.DefaultDate

	if e.HasEmptyPayload() {
		evType = DELETE
	} else {
		evType = UPDATE
		notificationPayloadMap, ok := e.Payload.(map[string]interface{})
		if ok {
			date = content.GetDateOrDefault(notificationPayloadMap)
		}
	}

	return &Notification{
		Stub: content.Stub{
			Uuid: UUID,
			Date: date,
		},
		EvType:     evType,
		Terminator: export.NewTerminator(),
	}, nil
}

func (h *KafkaMessageHandler) HandleNotificationEvent(n *Notification) {
	logEntry := log.WithField("transaction_id", n.Tid).WithField("uuid", n.Stub.Uuid)
	if n.EvType == UPDATE {
		logEntry.Infof("UPDATE event received. Waiting configured delay - %v second(s)", h.Delay)
		select {
		case <-time.After(time.Duration(h.Delay) * time.Second):
		case <-n.Quit:
			logEntry.WithError(errors.New("Shutdown signalled")).Error("FAILED UPDATE event")
			return
		}
		if err := h.ContentExporter.HandleContent(n.Tid, n.Stub); err != nil {
			logEntry.WithError(err).Error("FAILED UPDATE event")
		}
	} else if n.EvType == DELETE {
		logEntry.Info("DELETE event received")
		if err := h.ContentExporter.Updater.Delete(n.Stub.Uuid, n.Tid); err != nil {
			logEntry.WithError(err).Error("FAILED DELETE event")
		}
	}
}
