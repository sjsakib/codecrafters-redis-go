package engine

import "testing"

func TestLocation_Score(t *testing.T) {
	tests := []struct {
		name     string
		score   float64
		location Location
	}{
		{
			name: "Paris",
			score: 3663832614298053.0,
			location: Location{
				Label:     "Paris",
				Longitude: 2.2944692,
				Latitude:  48.8584625,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			score := tt.location.Score()
			if score != tt.score {
				t.Errorf("expected score %f, got %f", tt.score, score)
			}
		})
	}

}
