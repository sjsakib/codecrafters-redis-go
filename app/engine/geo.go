package engine

import (
	"fmt"
	"strconv"

	"github.com/codecrafters-io/redis-starter-go/app/resp"
)

const (
	MAX_LATITUDE  = 85.05112878
	MIN_LATITUDE  = -85.05112878
	LATITUDE_RANGE = MAX_LATITUDE - MIN_LATITUDE
	MAX_LONGITUDE = 180.0
	MIN_LONGITUDE = -180.0
	LONGITUDE_RANGE = MAX_LONGITUDE - MIN_LONGITUDE
)

type Location struct {
	Label     string
	Longitude float64
	Latitude  float64
}

func (l Location) Score() float64 {
	normalizedLongitude := int64(((l.Longitude - MIN_LONGITUDE) / LONGITUDE_RANGE) * (1 << 26))
	normalizedLatitude := int64(((l.Latitude - MIN_LATITUDE) / LATITUDE_RANGE) * (1 << 26))

	return interleave(normalizedLatitude, normalizedLongitude)
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

		isLatValid := latitude >= MIN_LATITUDE && latitude <= MAX_LATITUDE
		isLongValid := longitude >= MIN_LONGITUDE && longitude <= MAX_LONGITUDE
		if !isLatValid || !isLongValid {
			return resp.EncodeErrorMessage(fmt.Sprintf("invalid longitude,latitude pair %f,%f", longitude, latitude))
		}

		location.Longitude = longitude
		location.Latitude = latitude
		location.Label = command[i+2]
		score := location.Score()
		if set.Add(score, location.Label) {
			addedCount++
		}

	}

	return resp.EncodeResp(addedCount)
}

func interleave(x int64, y int64) float64 {
	x = spreadBits(x)
	y = spreadBits(y)

	y_shifted := y << 1

	return float64(x | y_shifted)
}
func spreadBits(x int64) int64 {
	x &= 0xFFFFFFFF

	x = (x | (x << 16)) & 0x0000FFFF0000FFFF
	x = (x | (x << 8)) & 0x00FF00FF00FF00FF
	x = (x | (x << 4)) & 0x0F0F0F0F0F0F0F0F
	x = (x | (x << 2)) & 0x3333333333333333
	x = (x | (x << 1)) & 0x5555555555555555

	return x
}