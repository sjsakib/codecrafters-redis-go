package utils

import "math"

const (
	MAX_LATITUDE    = 85.05112878
	MIN_LATITUDE    = -85.05112878
	LATITUDE_RANGE  = MAX_LATITUDE - MIN_LATITUDE
	MAX_LONGITUDE   = 180.0
	MIN_LONGITUDE   = -180.0
	LONGITUDE_RANGE = MAX_LONGITUDE - MIN_LONGITUDE
	EARTH_RADIUS    = 6372797.560856
)

type Location struct {
	Longitude float64
	Latitude  float64
}

func (l *Location) Score() float64 {
	normalizedLongitude := int64(((l.Longitude - MIN_LONGITUDE) / LONGITUDE_RANGE) * (1 << 26))
	normalizedLatitude := int64(((l.Latitude - MIN_LATITUDE) / LATITUDE_RANGE) * (1 << 26))

	return interleave(normalizedLatitude, normalizedLongitude)
}

func (l *Location) IsValid() bool {
	return l.Latitude >= MIN_LATITUDE && l.Latitude <= MAX_LATITUDE && l.Longitude >= MIN_LONGITUDE && l.Longitude <= MAX_LONGITUDE
}

func (l *Location) DistanceTo(other *Location) float64 {
	if !l.IsValid() || !other.IsValid() {
		return -1
	}
	return haversineDistance(l.Latitude, l.Longitude, other.Latitude, other.Longitude)
}

func NewLocation(longitude, latitude float64) *Location {
	return &Location{
		Longitude: longitude,
		Latitude:  latitude,
	}
}

func NewLocationFromScore(score float64) *Location {
	longitude, latitude := decodeGeoScore(score)
	return &Location{
		Longitude: longitude,
		Latitude:  latitude,
	}
}

func decodeGeoScore(geoScore float64) (float64, float64) {
	normalizedLatitude, normalizedLongitude := deInterleave(geoScore)
	lonMin := (float64(normalizedLongitude)*LONGITUDE_RANGE)/(1<<26) + MIN_LONGITUDE
	lonMax := (float64(normalizedLongitude+1)*LONGITUDE_RANGE)/(1<<26) + MIN_LONGITUDE
	latMin := (float64(normalizedLatitude)*LATITUDE_RANGE)/(1<<26) + MIN_LATITUDE
	latMax := (float64(normalizedLatitude+1)*LATITUDE_RANGE)/(1<<26) + MIN_LATITUDE

	return (lonMax + lonMin) / 2, (latMax + latMin) / 2
}

func interleave(x int64, y int64) float64 {
	x = spreadBits(x)
	y = spreadBits(y)

	y_shifted := y << 1

	return float64(x | y_shifted)
}

func deInterleave(z float64) (int64, int64) {
	zInt := int64(z)
	x := compactBits(zInt)
	y := compactBits(zInt >> 1)

	return x, y
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

func compactBits(x int64) int64 {
	x &= 0x5555555555555555
	x = (x | (x >> 1)) & 0x3333333333333333
	x = (x | (x >> 2)) & 0x0F0F0F0F0F0F0F0F
	x = (x | (x >> 4)) & 0x00FF00FF00FF00FF
	x = (x | (x >> 8)) & 0x0000FFFF0000FFFF
	x = (x | (x >> 16)) & 0x00000000FFFFFFFF

	return x
}

func haversineDistance(lat1, lon1, lat2, lon2 float64) float64 {
	phi1 := lat1 * (math.Pi / 180)
	phi2 := lat2 * (math.Pi / 180)
	deltaPhi := (lat2 - lat1) * (math.Pi / 180)
	deltaLambda := (lon2 - lon1) * (math.Pi / 180)

	a := math.Sin(deltaPhi/2)*math.Sin(deltaPhi/2) +
		math.Cos(phi1)*math.Cos(phi2)*
			math.Sin(deltaLambda/2)*math.Sin(deltaLambda/2)
	c := 2 * math.Atan2(math.Sqrt(a), math.Sqrt(1-a))

	return EARTH_RADIUS * c
}
