package db

import (
	"os"
	"testing"
	"strings"
	"github.com/stretchr/testify/assert"
	"context"
	"github.com/stretchr/testify/require"
	"github.com/pborman/uuid"
	"gopkg.in/mgo.v2/bson"
)

func startMongo(t *testing.T) Service {
	if testing.Short() {
		t.Skip("Mongo integration for long tests only.")
	}

	mongoURL := os.Getenv("MONGO_TEST_URL")
	if strings.TrimSpace(mongoURL) == "" {
		t.Fatal("Please set the environment variable MONGO_TEST_URL to run mongo integration tests (e.g. MONGO_TEST_URL=localhost:27017). Alternatively, run `go test -short` to skip them.")
	}

	return NewMongoDatabase(mongoURL, 30000)
}

func TestCreateDB(t *testing.T) {
	mongo := NewMongoDatabase("test-url", 30000)
	assert.Equal(t, "test-url", mongo.Urls)
	assert.Equal(t, 30000, mongo.Timeout)
	assert.NotNil(t, mongo.lock)
}

func TestPing(t *testing.T) {
	mongo := startMongo(t)
	defer mongo.Close()

	tx, err := mongo.Open()
	defer tx.Close()
	assert.NoError(t, err)

	err = tx.Ping(context.Background())
	assert.NoError(t, err)
}

func TestDBCloses(t *testing.T) {
	mongo := startMongo(t)
	tx, err := mongo.Open()
	assert.NoError(t, err)

	tx.Close()
	mongo.Close()
	assert.Panics(t, func() {
		mongo.(*MongoDB).session.Ping()
	})
}

func TestDBCheckHealth(t *testing.T) {
	mongo := startMongo(t)
	defer mongo.Close()
	tx, err := mongo.Open()
	defer tx.Close()
	assert.NoError(t, err)

	output, err := mongo.(*MongoDB).CheckHealth()
	assert.NoError(t, err)
	assert.Equal(t, "OK", output)
}

func TestFindUUIDsWithOnlyType(t *testing.T) {
	mongo := startMongo(t)
	defer mongo.Close()
	tx, err := mongo.Open()
	defer tx.Close()
	assert.NoError(t, err)

	testUUID := uuid.NewUUID().String()
	t.Log("Test uuid to use", testUUID)
	testContent := make(map[string]interface{})

	testContent["uuid"] = testUUID
	testContent["type"] = "Article"
	insertTestContent(t, mongo.(*MongoDB), testContent)
	defer cleanupTestContent(t, mongo.(*MongoDB), testUUID)

	iter, count, err := tx.FindUUIDs("testing", nil)
	require.NoError(t, err)
	defer iter.Close()
	require.NoError(t, iter.Err())
	require.Equal(t, 1, count)

	var result map[string]interface{}
	assert.True(t, iter.Next(&result))
	assert.Equal(t, testUUID, result["uuid"].(string))
}

func TestFindUUIDsWithOnlyBody(t *testing.T) {
	mongo := startMongo(t)
	defer mongo.Close()
	tx, err := mongo.Open()
	defer tx.Close()
	assert.NoError(t, err)

	testUUID := uuid.NewUUID().String()
	t.Log("Test uuid to use", testUUID)
	testContent := make(map[string]interface{})

	testContent["uuid"] = testUUID
	testContent["body"] = "some text"
	insertTestContent(t, mongo.(*MongoDB), testContent)
	defer cleanupTestContent(t, mongo.(*MongoDB), testUUID)

	iter, count, err := tx.FindUUIDs("testing", nil)
	require.NoError(t, err)
	defer iter.Close()
	require.NoError(t, iter.Err())
	require.Equal(t, 1, count)

	var result map[string]interface{}
	assert.True(t, iter.Next(&result))
	assert.Equal(t, testUUID, result["uuid"].(string))
}

func TestFindUUIDsWithOnlyRealtime(t *testing.T) {
	mongo := startMongo(t)
	defer mongo.Close()
	tx, err := mongo.Open()
	defer tx.Close()
	assert.NoError(t, err)

	testUUID := uuid.NewUUID().String()
	t.Log("Test uuid to use", testUUID)
	testContent := make(map[string]interface{})

	testContent["uuid"] = testUUID
	testContent["realtime"] = true
	insertTestContent(t, mongo.(*MongoDB), testContent)
	defer cleanupTestContent(t, mongo.(*MongoDB), testUUID)

	iter, count, err := tx.FindUUIDs("testing", nil)
	require.NoError(t, err)
	defer iter.Close()
	require.NoError(t, iter.Err())
	require.Equal(t, 1, count)

	var result map[string]interface{}
	assert.True(t, iter.Next(&result))
	assert.Equal(t, testUUID, result["uuid"].(string))
}

func TestFindUUIDsWithCanBeDistributedYes(t *testing.T) {
	mongo := startMongo(t)
	defer mongo.Close()
	tx, err := mongo.Open()
	defer tx.Close()
	assert.NoError(t, err)

	testUUID := uuid.NewUUID().String()
	t.Log("Test uuid to use", testUUID)
	testContent := make(map[string]interface{})

	testContent["uuid"] = testUUID
	testContent["type"] = "Article"
	testContent["canBeDistributed"] = "yes"
	insertTestContent(t, mongo.(*MongoDB), testContent)
	defer cleanupTestContent(t, mongo.(*MongoDB), testUUID)

	iter, count, err := tx.FindUUIDs("testing", nil)
	require.NoError(t, err)
	defer iter.Close()
	require.NoError(t, iter.Err())
	require.Equal(t, 1, count)

	var result map[string]interface{}
	assert.True(t, iter.Next(&result))
	assert.Equal(t, testUUID, result["uuid"].(string))
}

func TestFindUUIDsWithoutValidData(t *testing.T) {
	mongo := startMongo(t)
	defer mongo.Close()
	tx, err := mongo.Open()
	defer tx.Close()
	assert.NoError(t, err)

	testUUID := uuid.NewUUID().String()
	t.Log("Test uuid to use", testUUID)
	testContent := make(map[string]interface{})

	testContent["uuid"] = testUUID
	testContent["aProperty"] = "some text"
	insertTestContent(t, mongo.(*MongoDB), testContent)
	defer cleanupTestContent(t, mongo.(*MongoDB), testUUID)

	iter, count, err := tx.FindUUIDs("testing", nil)
	require.NoError(t, err)
	defer iter.Close()
	require.NoError(t, iter.Err())
	require.Equal(t, 0, count)

	var result map[string]interface{}
	assert.False(t, iter.Next(&result))
}

func insertTestContent(t *testing.T, mongo *MongoDB, testContent map[string]interface{}) {
	session := mongo.session.Copy()
	defer session.Close()

	err := session.DB("upp-store").C("testing").Insert(testContent)
	assert.NoError(t, err)
}

func cleanupTestContent(t *testing.T, mongo *MongoDB, testUUIDs ...string) {
	session := mongo.session.Copy()
	defer session.Close()
	for _, testUUID := range testUUIDs {
		err := session.DB("upp-store").C("testing").Remove(bson.M{"uuid": testUUID})
		assert.NoError(t, err)
	}
}
