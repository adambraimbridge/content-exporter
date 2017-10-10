package content

import (
	"encoding/json"
	"github.com/gorilla/mux"
	"github.com/pborman/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func (m *mockS3WriterServer) startMockS3WriterServer(t *testing.T) *httptest.Server {
	router := mux.NewRouter()
	router.HandleFunc("/content/{uuid}", func(w http.ResponseWriter, r *http.Request) {
		ua := r.Header.Get("User-Agent")
		assert.Equal(t, "UPP Content Exporter", ua, "user-agent header")

		contentTypeHeader := r.Header.Get("Content-Type")
		tid := r.Header.Get("X-Request-Id")

		pathUuid, ok := mux.Vars(r)["uuid"]
		assert.NotNil(t, pathUuid)
		assert.True(t, ok)

		body := make(map[string]interface{})
		dec := json.NewDecoder(r.Body)
		err := dec.Decode(&body)

		assert.NoError(t, err)

		bodyUuid, ok := body["uuid"]
		assert.NotNil(t, bodyUuid)
		assert.True(t, ok)

		date := r.URL.Query().Get("date")

		w.WriteHeader(m.UploadRequest(pathUuid, tid, contentTypeHeader, date))

	}).Methods(http.MethodPut)

	router.HandleFunc("/content/{uuid}", func(w http.ResponseWriter, r *http.Request) {
		ua := r.Header.Get("User-Agent")
		assert.Equal(t, "UPP Content Exporter", ua, "user-agent header")

		tid := r.Header.Get("X-Request-Id")

		pathUuid, ok := mux.Vars(r)["uuid"]
		assert.NotNil(t, pathUuid)
		assert.True(t, ok)

		w.WriteHeader(m.DeleteRequest(pathUuid, tid))

	}).Methods(http.MethodDelete)

	router.HandleFunc("/__gtg", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(m.GTG())
	}).Methods(http.MethodGet)

	return httptest.NewServer(router)
}

func (m *mockS3WriterServer) GTG() int {
	args := m.Called()
	return args.Int(0)
}

func (m *mockS3WriterServer) UploadRequest(bodyUuid, tid, contentTypeHeader, date string) int {
	args := m.Called(bodyUuid, tid, contentTypeHeader, date)
	return args.Int(0)
}

func (m *mockS3WriterServer) DeleteRequest(bodyUuid, tid string) int {
	args := m.Called(bodyUuid, tid)
	return args.Int(0)
}

type mockS3WriterServer struct {
	mock.Mock
}

func TestS3UpdaterUploadContent(t *testing.T) {
	testUUID := uuid.NewUUID().String()
	testData := make(map[string]interface{})
	testData["uuid"] = testUUID
	date := time.Now().UTC().Format("2006-01-02")

	mockServer := new(mockS3WriterServer)
	mockServer.On("UploadRequest", testUUID, "tid_1234", "application/json", date).Return(200)
	server := mockServer.startMockS3WriterServer(t)

	updater := NewS3Updater(server.URL)

	err := updater.Upload(testData, "tid_1234", testUUID, date)
	assert.NoError(t, err)
	mockServer.AssertExpectations(t)
}

func TestS3UpdaterUploadContentErrorResponse(t *testing.T) {
	testUUID := uuid.NewUUID().String()
	testData := make(map[string]interface{})
	testData["uuid"] = testUUID
	date := time.Now().UTC().Format("2006-01-02")

	mockServer := new(mockS3WriterServer)
	mockServer.On("UploadRequest", testUUID, "tid_1234", "application/json", date).Return(503)
	server := mockServer.startMockS3WriterServer(t)

	updater := NewS3Updater(server.URL)

	err := updater.Upload(testData, "tid_1234", testUUID, date)
	assert.Error(t, err)
	assert.Equal(t, "Content RW S3 returned HTTP 503", err.Error())
	mockServer.AssertExpectations(t)
}

func TestS3UpdaterUploadContentErrorParsing(t *testing.T) {
	testUUID := uuid.NewUUID().String()
	date := time.Now().UTC().Format("2006-01-02")

	mockServer := new(mockS3WriterServer)

	server := mockServer.startMockS3WriterServer(t)

	updater := NewS3Updater(server.URL)

	err := updater.Upload(map[string]interface{}{"": make(chan int)}, "tid_1234", testUUID, date)
	assert.Error(t, err)
	assert.Equal(t, "json: unsupported type: chan int", err.Error())
	mockServer.AssertExpectations(t)
}

func TestS3UpdaterDeleteContent(t *testing.T) {
	testUUID := uuid.NewUUID().String()

	mockServer := new(mockS3WriterServer)
	mockServer.On("DeleteRequest", testUUID, "tid_1234").Return(200)
	server := mockServer.startMockS3WriterServer(t)

	updater := NewS3Updater(server.URL)

	err := updater.Delete(testUUID, "tid_1234")
	assert.NoError(t, err)
	mockServer.AssertExpectations(t)
}

func TestS3UpdaterDeleteContentErrorResponse(t *testing.T) {
	testUUID := uuid.NewUUID().String()

	mockServer := new(mockS3WriterServer)
	mockServer.On("DeleteRequest", testUUID, "tid_1234").Return(503)
	server := mockServer.startMockS3WriterServer(t)

	updater := NewS3Updater(server.URL)

	err := updater.Delete(testUUID, "tid_1234")
	assert.Error(t, err)
	assert.Equal(t, "Content RW S3 returned HTTP 503", err.Error())
	mockServer.AssertExpectations(t)
}

func TestS3UpdaterCheckHealth(t *testing.T) {
	mockServer := new(mockS3WriterServer)
	mockServer.On("GTG").Return(200)
	server := mockServer.startMockS3WriterServer(t)

	updater := NewS3Updater(server.URL)

	resp, err := updater.(*S3Updater).CheckHealth()
	assert.NoError(t, err)
	assert.Equal(t, "S3 Writer is good to go.", resp)
	mockServer.AssertExpectations(t)
}

func TestS3UpdaterCheckHealthError(t *testing.T) {
	mockServer := new(mockS3WriterServer)
	mockServer.On("GTG").Return(503)
	server := mockServer.startMockS3WriterServer(t)

	updater := NewS3Updater(server.URL)

	resp, err := updater.(*S3Updater).CheckHealth()
	assert.Error(t, err)
	assert.Equal(t, "S3 Writer is not good to go.", resp)
	mockServer.AssertExpectations(t)
}

func NewS3Updater(s3WriterBaseURL string) Updater {
	return &S3Updater{Client: &http.Client{},
		S3WriterBaseURL:   s3WriterBaseURL,
		S3WriterHealthURL: s3WriterBaseURL + "/__gtg",
	}
}
