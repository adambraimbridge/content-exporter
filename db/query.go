package db

import (
	"gopkg.in/mgo.v2/bson"
)

var uuidProjection = bson.M{
	"uuid": 1,
}

func findUUIDsQueryElements() (bson.M, bson.M) {
	return bson.M{
		"$or": []bson.M{
			{"type": "Article"},
			{"body": bson.M{"$ne": nil}},
			{"realtime": true},
		},
	}, uuidProjection
}
