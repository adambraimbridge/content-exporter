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

type ContentNotificationHandler interface {
	HandleContentNotification(n *Notification) error
}

type KafkaMessageMapper struct {
	WhiteListRegex *regexp.Regexp
}

type KafkaContentNotificationHandler struct {
	ContentExporter *content.Exporter
	Delay           int
}

func NewKafkaContentNotificationHandler(exporter *content.Exporter, delayForNotification int) *KafkaContentNotificationHandler {
	return &KafkaContentNotificationHandler{
		ContentExporter: exporter,
		Delay:           delayForNotification,

	}
}

func (e PublicationEvent) MapNotification() (*Notification, error) {
	UUID := UUIDRegexp.FindString(e.ContentURI)
	if UUID == "" {
		return &Notification{Stub: content.Stub{}, EvType: EventType("")}, fmt.Errorf("ContentURI does not contain a UUID")
	}

	var evType EventType
	var payload map[string]interface{}

	if e.HasEmptyPayload() {
		evType = DELETE
	} else {
		evType = UPDATE
		notificationPayloadMap, ok := e.Payload.(map[string]interface{})
		if ok {
			payload = notificationPayloadMap
		}
	}

	return &Notification{
		Stub: content.Stub{
			Uuid: UUID,
			Date: content.GetDateOrDefault(payload),
		},
		EvType:     evType,
		Terminator: export.NewTerminator(),
	}, nil
}

func (h *KafkaContentNotificationHandler) HandleContentNotification(n *Notification) error {
	logEntry := log.WithField("transaction_id", n.Tid).WithField("uuid", n.Stub.Uuid)
	if n.EvType == UPDATE {
		logEntry.Infof("UPDATE event received. Waiting configured delay - %v second(s)", h.Delay)

		select {
		case <-time.After(time.Duration(h.Delay) * time.Second):
		case <-n.Quit:
			err := errors.New("Shutdown signalled, delay waiting for UPDATE event terminated abruptly")
			return err
		}
		if err := h.ContentExporter.HandleContent(n.Tid, n.Stub); err != nil {
			return fmt.Errorf("UPDATE ERROR: %v", err)
		}
	} else if n.EvType == DELETE {
		logEntry.Info("DELETE event received")
		if err := h.ContentExporter.Updater.Delete(n.Stub.Uuid, n.Tid); err != nil {
			return fmt.Errorf("DELETE ERROR: %v", err)
		}
	}
	return nil
}

func (h *KafkaMessageMapper) MapMessage(msg Message, tid string) (*Notification, error) {
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
