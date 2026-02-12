package storage

import (
	"errors"
	"time"
)

var (
	ErrKeyNotFound = errors.New("key not found")
	ErrWrongType   = errors.New("wrong type for operation")
)

type Storage interface {
	Get(key string) (any, bool)
	Set(key string, value any)
	GetList(key string) ([]any, error)
	GetOrMakeList(key string) ([]any, error)
	GetStream(key string) (*Stream, error)
	GetOrMakeStream(key string) (*Stream, error)
	GetStreamTopID(key string) (*EntryID, error)
	Expire(key string, duration time.Duration) bool
	ExpireTime(key string, duration time.Time) bool
	GetMatchingKeys(pattern string) ([]string, error)
}

type inMemoryStorage struct {
	data        map[string]any
	expirations map[string]time.Time
}

func NewInMemoryStorage() Storage {
	return &inMemoryStorage{
		data:        make(map[string]any),
		expirations: make(map[string]time.Time),
	}
}

func (s *inMemoryStorage) Get(key string) (any, bool) {
	expiration, exists := s.expirations[key]
	if exists && time.Now().After(expiration) {
		delete(s.data, key)
		delete(s.expirations, key)
		return "", false
	}
	value, exists := s.data[key]
	return value, exists
}

func (s *inMemoryStorage) GetList(key string) ([]any, error) {
	value, exists := s.data[key]
	if !exists {
		return nil, ErrKeyNotFound
	}
	list, ok := value.([]any)
	if !ok {
		return nil, ErrWrongType
	}
	return list, nil
}

func (s *inMemoryStorage) GetOrMakeList(key string) ([]any, error) {
	value, exists := s.data[key]
	if !exists {
		return make([]any, 0, 10), nil
	}
	list, ok := value.([]any)
	if !ok {
		return nil, ErrWrongType
	}
	return list, nil
}

func (s *inMemoryStorage) GetStream(key string) (*Stream, error) {
	value, exists := s.data[key]
	if !exists {
		return nil, ErrKeyNotFound
	}
	stream, ok := value.(*Stream)
	if !ok {
		return nil, ErrWrongType
	}
	return stream, nil
}

func (s *inMemoryStorage) GetOrMakeStream(key string) (*Stream, error) {
	value, exists := s.data[key]
	if !exists {
		return &Stream{Entries: make([]StreamEntry, 0, 10)}, nil
	}
	stream, ok := value.(*Stream)
	if !ok {
		return nil, ErrWrongType
	}
	return stream, nil
}

func (s *inMemoryStorage) GetStreamTopID(key string) (*EntryID, error) {
	stream, err := s.GetStream(key)
	if err != nil {
		if err == ErrKeyNotFound {
			return &EntryID{S: 0, T: 0}, nil
		}
		return nil, err
	}
	if len(stream.Entries) == 0 {
		return &EntryID{S: 0, T: 0}, nil
	}
	topEntry := stream.Entries[len(stream.Entries)-1]
	return &topEntry.ID, nil
}

func (s *inMemoryStorage) Set(key string, value any) {
	s.data[key] = value
	delete(s.expirations, key)
}

func (s *inMemoryStorage) Expire(key string, duration time.Duration) bool {
	_, exists := s.data[key]
	if !exists {
		return false
	}
	s.expirations[key] = time.Now().Add(duration)
	return true
}

func (s *inMemoryStorage) ExpireTime(key string, duration time.Time) bool {
	_, exists := s.data[key]
	if !exists {
		return false
	}
	s.expirations[key] = duration
	return true
}

func (s *inMemoryStorage) GetMatchingKeys(pattern string) ([]string, error) {
	matchingKeys := []string{}
	for key := range s.data {
		if matchPattern(key, pattern) {
			matchingKeys = append(matchingKeys, key)
		}
	}
	return matchingKeys, nil
}

func matchPattern(key, pattern string) bool {
	if pattern == "*" {
		return true
	}
	return key == pattern
}
