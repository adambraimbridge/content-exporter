package db

import (
	"gopkg.in/mgo.v2/bson"
)

var uuidProjection = bson.M{
	"uuid":               1,
	"firstPublishedDate": 1,
	"publishedDate":      1,
}

func findUUIDsQueryElements() (bson.M, bson.M) {
	return bson.M{
		"$and": []bson.M{
			{"$or": []bson.M{
				{"canBeDistributed": "yes"},
				{"canBeDistributed": bson.M{"$exists": false}},
			}},
			{"$or": []bson.M{
				{"type": "Article"},
				{"body": bson.M{"$ne": nil}},
				{"realtime": true},
			}},
		},
	}, uuidProjection
}
