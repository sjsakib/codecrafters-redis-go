package engine

import (
	"strconv"

	"github.com/codecrafters-io/redis-starter-go/app/resp"
)

func (e *engine) handleZAdd(command []string) []byte {
	if len(command) < 4 || (len(command)-2)%2 != 0 {
		return resp.EncodeErrorMessage("wrong number of arguments for 'ZADD' command")
	}
	key := command[1]
	sortedSet, err := e.storage.GetOrMakeSortedSet(key)
	if err != nil {
		return resp.EncodeErrorMessage(err.Error())
	}

	addedCount := 0
	for i := 2; i < len(command); i += 2 {
		score, err := strconv.ParseFloat(command[i], 64)
		if err != nil {
			return resp.EncodeErrorMessage("score must be a float")
		}
		member := command[i+1]
		if sortedSet.Add(score, member) {
			addedCount++
		}
	}

	return resp.EncodeResp(int64(addedCount))
}

func (e *engine) handleZRank(command []string) []byte {
	if len(command) < 3 {
		return resp.EncodeErrorMessage("wrong number of arguments for 'ZRANK' command")
	}
	key := command[1]
	sortedSet, err := e.storage.GetOrMakeSortedSet(key)
	if err != nil {
		return resp.EncodeErrorMessage(err.Error())
	}
	
	member := command[2]
	rank := sortedSet.Rank(member)
	if rank == -1 {
		return resp.EncodeNull()
	}
	return resp.EncodeResp(int64(rank))
}
