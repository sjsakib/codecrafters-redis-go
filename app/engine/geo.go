package engine

import (
	"fmt"
	"strconv"

	"github.com/codecrafters-io/redis-starter-go/app/resp"
	"github.com/codecrafters-io/redis-starter-go/app/utils"
)

func (e *engine) handleGeoAdd(command []string) []byte {
	if len(command) < 5 || (len(command)-2)%3 != 0 {
		return resp.EncodeErrorMessage("wrong number of arguments for 'GEOADD' command")
	}
	key := command[1]
	addedCount := 0
	set, err := e.storage.GetOrMakeSortedSet(key)
	if err != nil {
		return resp.EncodeErrorMessage(err.Error())
	}
	for i := 2; i < len(command); i += 3 {
		longitude, err := strconv.ParseFloat(command[i], 64)
		if err != nil {
			return resp.EncodeErrorMessage("longitude must be a float")
		}
		latitude, err := strconv.ParseFloat(command[i+1], 64)
		if err != nil {
			return resp.EncodeErrorMessage("latitude must be a float")
		}

		location := utils.NewLocation(longitude, latitude)

		if !location.IsValid() {
			return resp.EncodeErrorMessage(fmt.Sprintf("invalid longitude,latitude pair %f,%f", longitude, latitude))
		}

		score := location.Score()
		if set.Add(score, command[i+2]) {
			addedCount++
		}

	}

	return resp.EncodeResp(addedCount)
}

func (e *engine) handleGeopos(command []string) []byte {
	if len(command) < 3 {
		return resp.EncodeErrorMessage("wrong number of arguments for 'GEOPOS' command")
	}
	key := command[1]
	set, err := e.storage.GetOrMakeSortedSet(key)
	if err != nil {
		return resp.EncodeErrorMessage(err.Error())
	}

	results := make([]any, len(command)-2)
	for i, member := range command[2:] {
		score, exists := set.GetScore(member)
		if !exists {
			results[i] = resp.EncodeNullArray()
			continue
		}
		loc := utils.NewLocationFromScore(score)
		if !loc.IsValid() {
			results[i] = resp.EncodeNullArray()
			continue
		}
		results[i] = []float64{loc.Longitude, loc.Latitude}
	}

	return resp.EncodeResp(results)
}

func (e *engine) handleGeoDist(command []string) []byte {
	if len(command) != 4 {
		return resp.EncodeErrorMessage("wrong number of arguments for 'GEODIST' command")
	}
	key := command[1]
	set, err := e.storage.GetOrMakeSortedSet(key)
	if err != nil {
		return resp.EncodeErrorMessage(err.Error())
	}
	
	member1 := command[2]
	member2 := command[3]
	
	score1, exists1 := set.GetScore(member1)
	score2, exists2 := set.GetScore(member2)
	if !exists1 || !exists2 {
		return resp.EncodeNull()
	}
	loc1 := utils.NewLocationFromScore(score1)
	loc2 := utils.NewLocationFromScore(score2)

	if !loc1.IsValid() || !loc2.IsValid() {
		return resp.EncodeNull()
	}


	return resp.EncodeResp(loc1.DistanceTo(loc2))
}

func (e *engine) handleGeoSearch(command []string) []byte {
	if len(command) < 5 {
		return resp.EncodeErrorMessage("wrong number of arguments for 'GEOSEARCH' command")
	}
	key := command[1]
	set, err := e.storage.GetOrMakeSortedSet(key)
	if err != nil {
		return resp.EncodeErrorMessage(err.Error())
	}
	if command[2] != "FROMLONLAT" {
		return resp.EncodeErrorMessage("only FROMLONLAT is supported in GEOSEARCH")
	}
	longitude, err := strconv.ParseFloat(command[3], 64)
	if err != nil {
		return resp.EncodeErrorMessage("longitude must be a float")
	}
	latitude, err := strconv.ParseFloat(command[4], 64)
	if err != nil {
		return resp.EncodeErrorMessage("latitude must be a float")
	}
	center := utils.NewLocation(longitude, latitude)
	if !center.IsValid() {
		return resp.EncodeErrorMessage(fmt.Sprintf("invalid longitude,latitude pair %f,%f", longitude, latitude))
	}
	if command[5] != "BYRADIUS" {
		return resp.EncodeErrorMessage("only BYRADIUS is supported in GEOSEARCH")
	}
	radius, err := strconv.ParseFloat(command[6], 64)
	if err != nil {
		return resp.EncodeErrorMessage("radius must be a float")
	}
	unit := "m"
	if len(command) > 7 {
		unit = command[7]
	}
	if unit == "km" {
		radius *= 1000
	}

	results := []string{}
	for _, member := range set.Range(0, -1) {
		score, _ := set.GetScore(member)
		loc := utils.NewLocationFromScore(score)
		if loc.DistanceTo(center) <= radius {
			results = append(results, member)
		}
	}

	return resp.EncodeResp(results)
}



