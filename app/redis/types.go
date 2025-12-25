package redis

type Stream struct {
	Entries []StreamEntry
}

type StreamEntry struct {
	ID     string
	Fields map[string]string
}
