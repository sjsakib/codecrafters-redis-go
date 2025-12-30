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
	ID     string
	Fields map[string]string
}

func (s *Stream) GenerateOrValidateEntryID(entryID string) (string, error) {
	regexp, _ := regexp.Compile(`^(\*|(\d+-(\*|\d+)))$`)

	if !regexp.MatchString(entryID) {
		return "", errors.New("invalid entry id")
	}

	var lastTimeStamp int64
	lastSequence := int64(-1)
	if len(s.Entries) != 0 {
		lastEntry := s.Entries[len(s.Entries)-1]
		fmt.Sscanf(lastEntry.ID, "%d-%d", &lastTimeStamp, &lastSequence)
	}

	newTimeStamp := time.Now().UnixMilli()
	newSequence := int64(0)
	if entryID == "*" {
		// defaults are already set
	} else if strings.HasSuffix(entryID, "-*") {
		fmt.Sscanf(entryID, "%d-*", &newTimeStamp)

		if newTimeStamp < lastTimeStamp {
			return "", errors.New("The ID specified in XADD is equal or smaller than the target stream top item")
		} else if newTimeStamp == lastTimeStamp {
			newSequence = lastSequence + 1
		}

		if newTimeStamp == 0 && newSequence == 0 {
			newSequence = 1
		}
	} else {
		fmt.Sscanf(entryID, "%d-%d", &newTimeStamp, &newSequence)

		if newTimeStamp == 0 && newSequence == 0 {
			return "", errors.New("The ID specified in XADD must be greater than 0-0")
		}

		if newTimeStamp < lastTimeStamp {
			return "", errors.New("The ID specified in XADD is equal or smaller than the target stream top item")
		} else if newTimeStamp == lastTimeStamp && newSequence <= lastSequence {
			return "", errors.New("The ID specified in XADD is equal or smaller than the target stream top item")
		}
	}

	return fmt.Sprintf("%d-%d", newTimeStamp, newSequence), nil
}
