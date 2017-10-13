package queue

import (
	"encoding/json"
	"github.com/Financial-Times/content-exporter/content"
	"github.com/Financial-Times/kafka-client-go/kafka"
	"github.com/pborman/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"regexp"
	"testing"
)

func NewMessageMapper() MessageMapper {
	return NewKafkaMessageMapper(regexp.MustCompile("http://methode-article-mapper/content/.*"))
}

func testMapDeleteMessageSuccessfully(t *testing.T, ev event, testUUID string) {
	messageMapper := NewMessageMapper()

	body, err := json.Marshal(ev)
	require.NoError(t, err)
	n, err := messageMapper.MapNotification(kafka.FTMessage{Body: string(body), Headers: map[string]string{"X-Request-Id": "tid_1234"}})
	assert.NoError(t, err)
	assert.Equal(t, DELETE, n.EvType)
	assert.Equal(t, "tid_1234", n.Tid)
	assert.Equal(t, testUUID, n.Stub.Uuid)
	assert.Equal(t, content.DefaultDate, n.Stub.Date)
}

func TestKafkaMessageMapperMapDeleteMessageSuccessfullyWithoutPayload(t *testing.T) {
	testUUID := uuid.New()
	testMapDeleteMessageSuccessfully(t, event{
		ContentURI: "http://methode-article-mapper/content/" + testUUID}, testUUID)
}

func TestKafkaMessageMapperMapDeleteMessageSuccessfullyWithEmptyPayload(t *testing.T) {
	testUUID := uuid.New()
	testMapDeleteMessageSuccessfully(t, event{
		ContentURI: "http://methode-article-mapper/content/" + testUUID,
		Payload:    map[string]interface{}{}}, testUUID)
}

func TestKafkaMessageMapperMapDeleteMessageSuccessfullyWithEmptyStringPayload(t *testing.T) {
	testUUID := uuid.New()
	testMapDeleteMessageSuccessfully(t, event{
		ContentURI: "http://methode-article-mapper/content/" + testUUID,
		Payload:    ""}, testUUID)
}

func TestKafkaMessageMapperMapUpdateMessageSuccessfully(t *testing.T) {
	messageMapper := NewMessageMapper()
	testUUID := uuid.New()
	body, err := json.Marshal(event{
		ContentURI: "http://methode-article-mapper/content/" + testUUID,
		Payload:    map[string]interface{}{"title": "This is a title", "type": "Article"}})
	require.NoError(t, err)

	n, err := messageMapper.MapNotification(kafka.FTMessage{Body: string(body), Headers: map[string]string{"X-Request-Id": "tid_1234"}})

	assert.NoError(t, err)
	assert.Equal(t, UPDATE, n.EvType)
	assert.Equal(t, "tid_1234", n.Tid)
	assert.Equal(t, testUUID, n.Stub.Uuid)
	assert.Equal(t, content.DefaultDate, n.Stub.Date)
}

func TestKafkaMessageMapperMapUpdateMessageNoUUIDError(t *testing.T) {
	messageMapper := NewMessageMapper()
	body, err := json.Marshal(event{
		ContentURI: "http://methode-article-mapper/content/",
		Payload:    map[string]interface{}{"title": "This is a title", "type": "Article"}})
	require.NoError(t, err)

	n, err := messageMapper.MapNotification(kafka.FTMessage{Body: string(body), Headers: map[string]string{"X-Request-Id": "tid_1234"}})

	assert.Error(t, err)
	assert.Nil(t, n)
}

func TestKafkaMessageMapperMapNotificationNotInWhiteListError(t *testing.T) {
	messageMapper := NewMessageMapper()
	body, err := json.Marshal(event{
		ContentURI: "http://wordpress-article-mapper/content/",
		Payload:    map[string]interface{}{"title": "This is a title", "type": "Article"}})
	require.NoError(t, err)

	n, err := messageMapper.MapNotification(kafka.FTMessage{Body: string(body), Headers: map[string]string{"X-Request-Id": "tid_1234"}})

	assert.NoError(t, err)
	assert.Nil(t, n)
}

func TestKafkaMessageMapperMapNotificationSyntheticError(t *testing.T) {
	messageMapper := NewMessageMapper()
	body, err := json.Marshal(event{
		ContentURI: "http://methode-article-mapper/content/",
		Payload:    map[string]interface{}{"title": "This is a title", "type": "Article"}})
	require.NoError(t, err)

	n, err := messageMapper.MapNotification(kafka.FTMessage{Body: string(body), Headers: map[string]string{"X-Request-Id": "SYNTH_tid_1234"}})

	assert.NoError(t, err)
	assert.Nil(t, n)
}

func TestKafkaMessageMapperMapNotificationMessageParseError(t *testing.T) {
	messageMapper := NewMessageMapper()

	n, err := messageMapper.MapNotification(kafka.FTMessage{Body: "random-text", Headers: map[string]string{"X-Request-Id": "SYNTH_tid_1234"}})

	assert.Error(t, err)
	assert.Equal(t, "invalid character 'r' looking for beginning of value", err.Error())
	assert.Nil(t, n)
}
