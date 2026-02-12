package engine

import "time"

type Request struct {
	Input              []byte
	Command            []string
	ResCh              chan *Response
	Timestamp          time.Time
	ConnId             string
	IsTimeoutScheduled bool
}

type Response struct {
	Data      []byte
	RetryWait *time.Duration
}

type ReplicationInfo struct {
	MasterAddress string
	ReplicationId string
	Offset        int64
}

type TimeOut struct {
	Req *Request
}

type WaitReq struct {
	Req          *Request
	NumReplicas  int
	OffsetTarget int64
	AckedCount   int
}

type Config struct {
	Dir        string
	DBFilename string
}
