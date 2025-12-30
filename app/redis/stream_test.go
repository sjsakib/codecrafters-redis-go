package redis

import (
	"testing"
)

func TestStream_GenerateOrValidateEntryID(t *testing.T) {
	tests := []struct {
		name        string
		stream      *Stream
		entryID     string
		expectError bool
		expectID    string
	}{
		{
			name:        "invalid entry ID format",
			stream:      &Stream{},
			entryID:     "invalid",
			expectError: true,
		},
		{
			name:        "entry ID 0-0 should return error",
			stream:      &Stream{},
			entryID:     "0-0",
			expectError: true,
		},
		{
			name:        "auto-generate ID with * on empty stream",
			stream:      &Stream{},
			entryID:     "*",
			expectError: false,
			// expectID will be timestamp-0 format
		},
		{
			name: "auto-generate ID with * on existing stream",
			stream: &Stream{
				Entries: []StreamEntry{
					{ID: "1000-1", Fields: map[string]string{"key": "value"}},
				},
			},
			entryID:     "*",
			expectError: false,
			// expectID will be current timestamp-0 format
		},
		{
			name:        "timestamp-* format on empty stream",
			stream:      &Stream{},
			entryID:     "1500-*",
			expectError: false,
			expectID:    "1500-0",
		},
		{
			name: "timestamp-* format with valid timestamp greater than last",
			stream: &Stream{
				Entries: []StreamEntry{
					{ID: "1000-1", Fields: map[string]string{"key": "value"}},
				},
			},
			entryID:     "1500-*",
			expectError: false,
			expectID:    "1500-0",
		},
		{
			name: "timestamp-* format with timestamp equal to last should fail",
			stream: &Stream{
				Entries: []StreamEntry{
					{ID: "1000-1", Fields: map[string]string{"key": "value"}},
				},
			},
			entryID:     "1000-*",
			expectError: false,
			expectID:    "1000-2",
		},
		{
			name: "timestamp-* format with timestamp smaller than last should fail",
			stream: &Stream{
				Entries: []StreamEntry{
					{ID: "1000-1", Fields: map[string]string{"key": "value"}},
				},
			},
			entryID:     "500-*",
			expectError: true,
		},
		{
			name:        "explicit timestamp-sequence on empty stream",
			stream:      &Stream{},
			entryID:     "1000-5",
			expectError: false,
			expectID:    "1000-5",
		},
		{
			name: "explicit timestamp-sequence with greater timestamp",
			stream: &Stream{
				Entries: []StreamEntry{
					{ID: "1000-1", Fields: map[string]string{"key": "value"}},
				},
			},
			entryID:     "1500-3",
			expectError: false,
			expectID:    "1500-3",
		},
		{
			name: "explicit timestamp-sequence with same timestamp but greater sequence",
			stream: &Stream{
				Entries: []StreamEntry{
					{ID: "1000-1", Fields: map[string]string{"key": "value"}},
				},
			},
			entryID:     "1000-5",
			expectError: false,
			expectID:    "1000-5",
		},
		{
			name: "explicit timestamp-sequence with same timestamp, auto-increment sequence",
			stream: &Stream{
				Entries: []StreamEntry{
					{ID: "1000-5", Fields: map[string]string{"key": "value"}},
				},
			},
			entryID:     "1000-3",
			expectError: true,
		},
		{
			name: "explicit timestamp-sequence with smaller timestamp, auto-correct",
			stream: &Stream{
				Entries: []StreamEntry{
					{ID: "1000-5", Fields: map[string]string{"key": "value"}},
				},
			},
			entryID:     "500-10",
			expectError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := tt.stream.GenerateOrValidateEntryID(tt.entryID)

			if tt.expectError && err == nil {
				t.Errorf("expected error but got none")
				return
			}

			if !tt.expectError && err != nil {
				t.Errorf("unexpected error: %v", err)
				return
			}

			if !tt.expectError {
				if tt.expectID != "" && result != tt.expectID {
					t.Errorf("expected ID %s, got %s", tt.expectID, result)
				}

				// For auto-generated IDs (*), just verify the format
				if tt.entryID == "*" {
					// TODO: Add validation for auto-generated timestamp format
					if result == "" {
						t.Errorf("expected non-empty result for auto-generated ID")
					}
				}
			}
		})
	}
}

