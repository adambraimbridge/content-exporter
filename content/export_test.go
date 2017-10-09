package content

import (
	"testing"
	"github.com/stretchr/testify/assert"
	"github.com/pkg/errors"
)

func TestExporterHandleContentWithValidContent(t *testing.T) {
	tid := "tid_1234"
	stubUuid := "uuid1"
	date := "2017-10-09"
	testData := make(map[string]interface{})
	testData["uuid"] = stubUuid
	fetcher := &mockFetcher{expectedUuid: stubUuid, expectedTid: tid, result: testData}
	updater := &mockUpdater{expectedUuid: stubUuid, expectedTid: tid, expectedDate: date, expectedPayload: testData}

	exporter := NewExporter(fetcher, updater)
	err := exporter.HandleContent(tid, Stub{stubUuid, date})

	assert.NoError(t, err)
	assert.True(t, fetcher.called)
	assert.True(t, updater.called)
}

func TestExporterHandleContentWithErrorFromFetcher(t *testing.T) {
	tid := "tid_1234"
	stubUuid := "uuid1"
	date := "2017-10-09"
	testData := make(map[string]interface{})
	testData["uuid"] = stubUuid
	fetcher := &mockFetcher{expectedUuid: stubUuid, expectedTid: tid, result: testData, err: errors.New("Fetcher err")}
	updater := &mockUpdater{}

	exporter := NewExporter(fetcher, updater)
	err := exporter.HandleContent(tid, Stub{stubUuid, date})

	assert.Error(t, err)
	assert.Equal(t, "Error by getting content for uuid1: Fetcher err", err.Error())
	assert.True(t, fetcher.called)
	assert.False(t, updater.called)
}

func TestExporterHandleContentWithErrorFromUpdater(t *testing.T) {
	tid := "tid_1234"
	stubUuid := "uuid1"
	date := "2017-10-09"
	testData := make(map[string]interface{})
	testData["uuid"] = stubUuid
	fetcher := &mockFetcher{expectedUuid: stubUuid, expectedTid: tid, result: testData}
	updater := &mockUpdater{expectedUuid: stubUuid, expectedTid: tid, expectedDate: date, expectedPayload: testData, err: errors.New("Updater err")}

	exporter := NewExporter(fetcher, updater)
	err := exporter.HandleContent(tid, Stub{stubUuid, date})

	assert.Error(t, err)
	assert.Equal(t, "Error by uploading content for uuid1: Updater err", err.Error())
	assert.True(t, fetcher.called)
	assert.True(t, updater.called)
}

type mockFetcher struct {
	t                         *testing.T
	expectedUuid, expectedTid string
	result                    map[string]interface{}
	err                       error
	called                    bool
}

func (f *mockFetcher) GetContent(uuid, tid string) (map[string]interface{}, error) {
	assert.Equal(f.t, f.expectedUuid, uuid)
	assert.Equal(f.t, f.expectedTid, tid)
	f.called = true
	return f.result, f.err
}

type mockUpdater struct {
	t                                       *testing.T
	expectedUuid, expectedTid, expectedDate string
	expectedPayload                         map[string]interface{}
	err                                     error
	called                                  bool
}

func (u *mockUpdater) Upload(content map[string]interface{}, tid, uuid, date string) error {
	assert.Equal(u.t, u.expectedUuid, uuid)
	assert.Equal(u.t, u.expectedTid, tid)
	assert.Equal(u.t, u.expectedDate, date)
	assert.Equal(u.t, u.expectedPayload, content)
	u.called = true
	return u.err
}

func (u *mockUpdater) Delete(uuid, tid string) error {
	panic("should not be called")
}

func TestGetDateWhenFirstPublishedDateIsPresent(t *testing.T) {
	expectedDate := "2006-01-02"
	firsPublishDate := expectedDate + "T15:04:05Z07:00"
	publishDate := "2017-01-19T15:04:05Z07:00"
	testData := make(map[string]interface{})
	testData["firstPublishedDate"] = firsPublishDate
	testData["publishedDate"] = publishDate

	actualDate := GetDateOrDefault(testData)
	assert.Equal(t, expectedDate, actualDate)
}

func TestGetDateWhenNoFirstPublishedDateButPublishDateIsPresent(t *testing.T) {
	expectedDate := "2017-01-19"
	publishDate := "2017-01-19T15:04:05Z07:00"
	testData := make(map[string]interface{})
	testData["publishedDate"] = publishDate

	actualDate := GetDateOrDefault(testData)
	assert.Equal(t, expectedDate, actualDate)
}

func TestGetDateWhenNeitherFirstPublishedDateNorPublishDateIsPresent(t *testing.T) {
	expectedDate := "0000-00-00"
	testData := make(map[string]interface{})

	actualDate := GetDateOrDefault(testData)
	assert.Equal(t, expectedDate, actualDate)
}