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
	messageConsumer            kafka.Consumer
	*export.Locker
	sync.RWMutex
	paused                     bool
	*export.Terminator
	received                   chan *Notification
	pending                    map[string]*Notification
	ContentNotificationHandler *KafkaContentNotificationHandler
	MessageMapper *KafkaMessageMapper
}

func NewKafkaListener(messageConsumer kafka.Consumer, notificationHandler *KafkaContentNotificationHandler, messageMapper *KafkaMessageMapper, locker *export.Locker) *KafkaListener {
	chanCap := 2
	return &KafkaListener{
		messageConsumer:            messageConsumer,
		Locker:                     locker,
		received:                   make(chan *Notification, chanCap),
		pending:                    make(map[string]*Notification, chanCap),
		Terminator:                 export.NewTerminator(),
		ContentNotificationHandler: notificationHandler,
		MessageMapper:              messageMapper,
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
	h.messageConsumer.StartListening(h.HandleMessage)
	go h.handleNotifications()

	defer func() {
		h.Terminator.ShutDownPrepared = true
		h.TerminatePendingNotifications()
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

func (h *KafkaListener) HandleMessage(queueMsg kafka.FTMessage) error {
	if h.ShutDownPrepared {
		h.Cleanup.Do(func() {
			close(h.received)
		})
		return fmt.Errorf("Service is shutdown")
	}

	tid := queueMsg.Headers["X-Request-Id"]

	if h.paused {
		log.WithField("transaction_id", tid).Info("PAUSED handling message")
		for h.paused {
			if h.ShutDownPrepared {
				h.Cleanup.Do(func() {
					close(h.received)
				})
				return fmt.Errorf("Service is shutdown")
			}
			time.Sleep(time.Millisecond * 500)
		}
		log.WithField("transaction_id", tid).Info("PAUSE finished. Resuming handling messages")
	}
	msg := Message{queueMsg}
	n, err := h.MessageMapper.MapMessage(msg, tid)
	if n.EvType != "" {
		h.received <- n
	}
	if h.ShutDownPrepared {
		h.Cleanup.Do(func() {
			close(h.received)
		})
	}
	return err
}

func (h *KafkaListener) handleNotifications() {
	log.Info("Started handling notifications")
	for n := range h.received {
		h.pending[n.Tid] = n
		if h.paused {
			log.WithField("transaction_id", n.Tid).Info("PAUSED handling notification")
			for h.paused {
				time.Sleep(time.Millisecond * 500)
			}
			log.WithField("transaction_id", n.Tid).Info("PAUSE finished. Resuming handling notification")
		}
		if err := h.ContentNotificationHandler.HandleContentNotification(n); err != nil {
			log.WithField("transaction_id", n.Tid).WithField("uuid", n.Stub.Uuid).WithError(err).Error("Failed notification handling")
		}
		delete(h.pending, n.Tid)
	}
	log.Info("Stopped handling notifications")
	h.ShutDown = true
}

func (h *KafkaListener) TerminatePendingNotifications() {
	for _, n := range h.pending {
		n.Quit <- struct{}{}
	}
}

func (h *KafkaListener) CheckHealth() (string, error) {
	if err := h.messageConsumer.ConnectivityCheck(); err != nil {
		return "Kafka is not good to go.", err
	}
	return "Kafka is good to go.", nil
}
