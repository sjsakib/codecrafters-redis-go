package redis

import (
	"errors"
	"fmt"
	"regexp"
	"strings"
	"time"
)

type Stream struct {
	Entries []StreamEntry
}

type StreamEntry struct {
	ID     EntryID
	Fields map[string]string
}

type EntryID struct {
	T int64
	S int64
}

func (s *Stream) GenerateOrValidateEntryID(entryID string) (*EntryID, error) {
	regexp, _ := regexp.Compile(`^(\*|(\d+-(\*|\d+)))$`)

	if !regexp.MatchString(entryID) {
		return nil, errors.New("invalid entry id")
	}

	var lastTimeStamp int64
	lastSequence := int64(-1)
	if len(s.Entries) != 0 {
		lastEntry := s.Entries[len(s.Entries)-1]
		lastTimeStamp = lastEntry.ID.T
		lastSequence = lastEntry.ID.S
	}

	newTimeStamp := time.Now().UnixMilli()
	newSequence := int64(0)
	if entryID == "*" {
		// defaults are already set
	} else if strings.HasSuffix(entryID, "-*") {
		fmt.Sscanf(entryID, "%d-*", &newTimeStamp)

		if newTimeStamp < lastTimeStamp {
			return nil, errors.New("The ID specified in XADD is equal or smaller than the target stream top item")
		} else if newTimeStamp == lastTimeStamp {
			newSequence = lastSequence + 1
		}

		if newTimeStamp == 0 && newSequence == 0 {
			newSequence = 1
		}
	} else {
		fmt.Sscanf(entryID, "%d-%d", &newTimeStamp, &newSequence)

		if newTimeStamp == 0 && newSequence == 0 {
			return nil, errors.New("The ID specified in XADD must be greater than 0-0")
		}

		if newTimeStamp < lastTimeStamp {
			return nil, errors.New("The ID specified in XADD is equal or smaller than the target stream top item")
		} else if newTimeStamp == lastTimeStamp && newSequence <= lastSequence {
			return nil, errors.New("The ID specified in XADD is equal or smaller than the target stream top item")
		}
	}

	return &EntryID{T: newTimeStamp, S: newSequence}, nil
}

func (s *Stream) GetRange(startID, endID EntryID) []StreamEntry {
	var result []StreamEntry
	for _, entry := range s.Entries {
		if (entry.ID.T > startID.T || (entry.ID.T == startID.T && entry.ID.S >= startID.S)) &&
			(entry.ID.T < endID.T || (entry.ID.T == endID.T && entry.ID.S <= endID.S)) {
			result = append(result, entry)
		}
	}
	return result
}

func parseRangeID(idStr string, isStart bool) (EntryID, error) {
	if idStr == "-" {
		return EntryID{T: 0, S: 0}, nil
	}
	if idStr == "+" {
		return EntryID{T: int64(^uint64(0) >> 1), S: int64(^uint64(0) >> 1)}, nil
	}
	parts := strings.Split(idStr, "-")
	var id EntryID

	_, err := fmt.Sscanf(parts[0], "%d", &id.T)
	if err != nil {
		return EntryID{}, errors.New("invalid ID format")
	}

	if len(parts) > 1 {
		_, err = fmt.Sscanf(parts[1], "%d", &id.S)
		if err != nil {
			return EntryID{}, errors.New("invalid ID format")
		}
	} else {
		if isStart {
			id.S = 0
		} else {
			id.S = int64(^uint64(0) >> 1) // max int64
		}
	}
	return id, nil
}
