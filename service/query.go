package service

import (
	"gopkg.in/mgo.v2/bson"
)

var uuidProjection = bson.M{
	"uuid": 1,
}

func findUUIDsQueryElements() (bson.M, bson.M) {
	return bson.M{"body": ""}, uuidProjection
}
