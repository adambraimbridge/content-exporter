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
)

func (m *mockEnrichedContentServer) startMockEnrichedContentServer(t *testing.T) *httptest.Server {
	router := mux.NewRouter()
	router.HandleFunc("/enrichedcontent/{uuid}", func(w http.ResponseWriter, r *http.Request) {
		ua := r.Header.Get("User-Agent")
		assert.Equal(t, "UPP Content Exporter", ua, "user-agent header")

		acceptHeader := r.Header.Get("Accept")
		tid := r.Header.Get("X-Request-Id")
		xPolicyHeader := r.Header.Get("X-Policy")
		authHeader := r.Header.Get("Authorization")

		uuid, ok := mux.Vars(r)["uuid"]
		assert.NotNil(t, uuid)
		assert.True(t, ok)

		respStatus, resp := m.GetRequest(authHeader, tid, acceptHeader, xPolicyHeader)

		w.WriteHeader(respStatus)
		if len(resp) != 0 {
			assert.NoError(t, json.NewEncoder(w).Encode(resp))
		}
	}).Methods(http.MethodGet)

	router.HandleFunc("/__gtg", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(m.GTG())
	}).Methods(http.MethodGet)

	return httptest.NewServer(router)
}

func (m *mockEnrichedContentServer) GTG() int {
	args := m.Called()
	return args.Int(0)
}

func (m *mockEnrichedContentServer) GetRequest(authHeader string, tid string, acceptHeader string, xPolicyHeader string) (int, map[string]interface{}) {
	args := m.Called(authHeader, tid, acceptHeader, xPolicyHeader)
	var resp map[string]interface{}
	if len(args) > 1 {
		resp = args.Get(1).(map[string]interface{})
	}
	return args.Int(0), resp
}

type mockEnrichedContentServer struct {
	mock.Mock
}

func TestGetValidContent(t *testing.T) {
	testUUID := uuid.NewUUID().String()
	testData := make(map[string]interface{})
	testData["uuid"] = testUUID
	mockServer := new(mockEnrichedContentServer)
	mockServer.On("GetRequest", "", "tid_1234", "application/json", "").Return(200, testData)

	server := mockServer.startMockEnrichedContentServer(t)

	fetcher := NewEnrichedContentFetcher(server.URL, "", "")

	resp, err := fetcher.GetContent(testUUID, "tid_1234")
	assert.NoError(t, err)
	mockServer.AssertExpectations(t)
	assert.True(t, len(resp) == 1)
	assert.Equal(t, testUUID, resp["uuid"].(string))
}

func TestGetValidContentWithAuthorizationAndXPolicy(t *testing.T) {
	testUUID := uuid.NewUUID().String()
	testData := make(map[string]interface{})
	testData["uuid"] = testUUID
	mockServer := new(mockEnrichedContentServer)
	auth := "auth-string"
	xPolicies := "xpolicies"
	mockServer.On("GetRequest", auth, "tid_1234", "application/json", xPolicies).Return(200, testData)

	server := mockServer.startMockEnrichedContentServer(t)

	fetcher := NewEnrichedContentFetcher(server.URL, auth, xPolicies)

	resp, err := fetcher.GetContent(testUUID, "tid_1234")
	assert.NoError(t, err)
	mockServer.AssertExpectations(t)
	assert.True(t, len(resp) == 1)
	assert.Equal(t, testUUID, resp["uuid"].(string))
}

func TestGetContentWithAuthError(t *testing.T) {
	testUUID := uuid.NewUUID().String()
	mockServer := new(mockEnrichedContentServer)
	auth := "auth-string"
	xPolicies := "xpolicies"
	mockServer.On("GetRequest", auth, "tid_1234", "application/json", xPolicies).Return(http.StatusUnauthorized)

	server := mockServer.startMockEnrichedContentServer(t)

	fetcher := NewEnrichedContentFetcher(server.URL, auth, xPolicies)

	_, err := fetcher.GetContent(testUUID, "tid_1234")
	assert.Error(t, err)
	mockServer.AssertExpectations(t)
	assert.Equal(t, "EnrichedContent returned HTTP 401", err.Error())
}

func TestGetContentWithForbiddenError(t *testing.T) {
	testUUID := uuid.NewUUID().String()
	mockServer := new(mockEnrichedContentServer)
	auth := "auth-string"
	xPolicies := "xpolicies"
	mockServer.On("GetRequest", auth, "tid_1234", "application/json", xPolicies).Return(http.StatusForbidden)

	server := mockServer.startMockEnrichedContentServer(t)

	fetcher := NewEnrichedContentFetcher(server.URL, auth, xPolicies)

	_, err := fetcher.GetContent(testUUID, "tid_1234")
	assert.Error(t, err)
	mockServer.AssertExpectations(t)
	assert.Equal(t, "Access to content is forbidden. Skipping", err.Error())
}

func TestCheckHealth(t *testing.T) {
	mockServer := new(mockEnrichedContentServer)
	mockServer.On("GTG").Return(200)
	server := mockServer.startMockEnrichedContentServer(t)

	fetcher := NewEnrichedContentFetcher(server.URL, "", "")

	resp, err := fetcher.(*EnrichedContentFetcher).CheckHealth()
	assert.NoError(t, err)
	assert.Equal(t, "EnrichedContent fetcher is good to go.", resp)
	mockServer.AssertExpectations(t)
}

func TestCheckHealthError(t *testing.T) {
	mockServer := new(mockEnrichedContentServer)
	mockServer.On("GTG").Return(503)
	server := mockServer.startMockEnrichedContentServer(t)

	fetcher := NewEnrichedContentFetcher(server.URL, "", "")

	resp, err := fetcher.(*EnrichedContentFetcher).CheckHealth()
	assert.Error(t, err)
	assert.Equal(t, "EnrichedContent fetcher is not good to go.", resp)
	mockServer.AssertExpectations(t)
}

func NewEnrichedContentFetcher(enrichedContentBaseURL, authorization, xPolicyHeaderValues string) Fetcher {
	return &EnrichedContentFetcher{Client: &http.Client{},
		EnrichedContentBaseURL:   enrichedContentBaseURL,
		EnrichedContentHealthURL: enrichedContentBaseURL + "/__gtg",
		XPolicyHeaderValues:      xPolicyHeaderValues,
		Authorization:            authorization,
	}
}
