package queue

import (
	"encoding/json"
	"fmt"
	"github.com/Financial-Times/content-exporter/export"
	"github.com/Financial-Times/kafka-client-go/kafka"
	log "github.com/sirupsen/logrus"
	"regexp"
	"strings"
	"sync"
	"time"
)

type MessageHandler interface {
	HandleMessage(queueMsg kafka.FTMessage) error
}

type KafkaListener struct {
	messageConsumer kafka.Consumer
	*export.Locker
	sync.RWMutex
	paused bool
	*export.Terminator
	notifCh             chan Notification
	KafkaMessageHandler *KafkaMessageHandler
}

func NewKafkaListener(messageConsumer kafka.Consumer, messageHandler *KafkaMessageHandler, locker *export.Locker) *KafkaListener {
	return &KafkaListener{
		messageConsumer:     messageConsumer,
		Locker:              locker,
		notifCh:             make(chan Notification, 2), //TODO when to close? is 30 as a buffer ok?
		Terminator:          export.NewTerminator(),
		KafkaMessageHandler: messageHandler,
	}
}

type Message struct {
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

func (msg Message) ToPublicationEvent() (event PublicationEvent, err error) {
	err = json.Unmarshal([]byte(msg.Body), &event)
	return event, err
}

func (msg Message) HasSynthTransactionID() bool {
	tid := msg.TransactionID()
	return strings.HasPrefix(tid, "SYNTH")
}

func (msg Message) TransactionID() string {
	return msg.Headers["X-Request-Id"]
}

func (h *KafkaListener) resumeConsuming() {
	h.Lock()
	defer h.Unlock()
	log.Debugf("DEBUG resumeConsuming")
	if h.paused {
		h.paused = false
	}
}

func (h *KafkaListener) pauseConsuming() {
	h.Lock()
	defer h.Unlock()
	log.Debugf("DEBUG pauseConsuming")
	if !h.paused {
		h.paused = true
	}
}

func (h *KafkaListener) ConsumeMessages() {
	h.messageConsumer.StartListening(h.handleMessage)
	go h.handleNotifications()

	defer func() {
		h.Terminator.ShutDownPrepared = true
		h.KafkaMessageHandler.Quit <- struct{}{}
	}()
	defer h.messageConsumer.Shutdown()

	for {
		select {
		case locked := <-h.Locker.Locked:
			log.Infof("LOCK signal received: %v...", locked)
			if locked {
				h.pauseConsuming()
				select {
				case h.Locker.Acked <- struct{}{}:
					log.Infof("LOCK acked")
				case <-time.After(time.Second * 3):
					log.Infof("LOCK acking timed out. Maybe initiator quit already?")
				}
			} else {
				h.resumeConsuming()
			}
		case <-h.Quit:
			log.Infof("QUIT signal received...")
			return
		}
	}
}

func (h *KafkaListener) StopConsumingMessages() {
	h.Quit <- struct{}{}
	for {
		if h.ShutDown {
			return
		}
		time.Sleep(time.Millisecond * 100)
	}
}

func (h *KafkaListener) handleMessage(queueMsg kafka.FTMessage) error {
	if h.ShutDownPrepared {
		h.Cleanup.Do(func() {
			close(h.notifCh)
		})
		return fmt.Errorf("Service is shutdown")
	}

	tid := queueMsg.Headers["X-Request-Id"]

	if h.paused {
		log.WithField("transaction_id", tid).Info("PAUSED handling message")
		for h.paused {
			time.Sleep(time.Millisecond * 500)
		}
		log.WithField("transaction_id", tid).Info("PAUSE finished. Resuming handling messages")
	}
	msg := Message{queueMsg}
	notif, err := h.KafkaMessageHandler.handleMessage(msg, tid)
	if notif.EvType != "" {
		h.notifCh <- notif
	}
	if h.ShutDownPrepared {
		h.Cleanup.Do(func() {
			close(h.notifCh)
		})
	}
	return err
}

func (h *KafkaListener) handleNotifications() {
	log.Info("Started handling notifications")
	for n := range h.notifCh {
		log.Debugf("DEBUG Len(notifCh) vs cap(notifCh) - %v vs %v", len(h.notifCh), cap(h.notifCh))
		if h.paused {
			log.WithField("transaction_id", n.Tid).Info("PAUSED handling notification")
			for h.paused {
				time.Sleep(time.Millisecond * 500)
			}
			log.WithField("transaction_id", n.Tid).Info("PAUSE finished. Resuming handling notification")
		}
		if h.ShutDownPrepared {
			log.WithField("transaction_id", n.Tid).WithField("uuid", n.Stub.Uuid).Error("Service is shutdown")
		}
		h.KafkaMessageHandler.HandleNotificationEvent(n)

	}
	log.Info("Stopped handling notifications")
	h.ShutDown = true
}

func (h *KafkaListener) CheckHealth() (string, error) {
	if err := h.messageConsumer.ConnectivityCheck(); err != nil {
		return "Kafka is not good to go.", err
	}
	return "Kafka is good to go.", nil
}
