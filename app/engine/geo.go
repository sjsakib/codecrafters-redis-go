package engine

import (
	"fmt"
	"strconv"

	"github.com/codecrafters-io/redis-starter-go/app/resp"
)

type Location struct {
	Label string
	Longitude float64
	Latitude  float64
}

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
		var location Location
		longitude, err := strconv.ParseFloat(command[i], 64)
		if err != nil {
			return resp.EncodeErrorMessage("longitude must be a float")
		}
		latitude, err := strconv.ParseFloat(command[i+1], 64)
		if err != nil {
			return resp.EncodeErrorMessage("latitude must be a float")
		}
		
		isLatValid := latitude >= -85.05112878 && latitude <= 85.05112878
		isLongValid := longitude >= -180 && longitude <= 180
		if !isLatValid || !isLongValid {
			return resp.EncodeErrorMessage(fmt.Sprintf("invalid longitude,latitude pair %f,%f", longitude, latitude))
		}

		location.Longitude = longitude
		location.Latitude = latitude
		location.Label = command[i+2]
		score := float64(0)
		if set.Add(score, location.Label) {
			addedCount++
		}

	}

	return resp.EncodeResp(addedCount)
}
