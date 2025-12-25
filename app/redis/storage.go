package redis

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
	Expire(key string, duration time.Duration) bool
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
