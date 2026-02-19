package engine

import "github.com/codecrafters-io/redis-starter-go/app/resp"

func (e *engine) handleGeoAdd(_command []string) []byte {
	return resp.EncodeResp(1)
}
