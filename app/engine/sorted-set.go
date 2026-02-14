package engine

import (
	"strconv"

	"github.com/codecrafters-io/redis-starter-go/app/resp"
	"github.com/codecrafters-io/redis-starter-go/app/storage"
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

func (e *engine) handleZRange(command []string) []byte {
	if len(command) < 4 {
		return resp.EncodeErrorMessage("wrong number of arguments for 'ZRANGE' command")
	}
	key := command[1]
	sortedSet, err := e.storage.GetOrMakeSortedSet(key)
	if err != nil {
		return resp.EncodeErrorMessage(err.Error())
	}
	
	start, err := strconv.Atoi(command[2])
	if err != nil {
		return resp.EncodeErrorMessage("start must be an integer")
	}
	stop, err := strconv.Atoi(command[3])
	if err != nil {
		return resp.EncodeErrorMessage("stop must be an integer")
	}
	
	members := sortedSet.Range(start, stop)
	return resp.EncodeArray(members)
}

func (e *engine) handleZCard(command []string) []byte {
	if len(command) < 2 {
		return resp.EncodeErrorMessage("wrong number of arguments for 'ZCARD' command")
	}
	key := command[1]
	sortedSet, err := e.storage.GetOrMakeSortedSet(key)
	if err != nil {
		return resp.EncodeErrorMessage(err.Error())
	}

	return resp.EncodeResp(int64(sortedSet.Size()))
}

func (e *engine) handleZScore(command []string) []byte {
	if len(command) < 3 {
		return resp.EncodeErrorMessage("wrong number of arguments for 'ZSCORE' command")
	}
	key := command[1]
	sortedSet, err := e.storage.GetSortedSet(key)
	if err != nil {
		if err == storage.ErrKeyNotFound {
			return resp.EncodeNull()
		}
		return resp.EncodeErrorMessage(err.Error())
	}
	
	member := command[2]
	score, exists := sortedSet.GetScore(member)
	if !exists {
		return resp.EncodeNull()
	}
	return resp.EncodeResp(score)
}

func (e *engine) handleZRem(command []string) []byte {
	if len(command) < 3 {
		return resp.EncodeErrorMessage("wrong number of arguments for 'ZREM' command")
	}
	key := command[1]
	sortedSet, err := e.storage.GetSortedSet(key)
	if err != nil {
		if err == storage.ErrKeyNotFound {
			return resp.EncodeResp(int64(0))
		}
		return resp.EncodeErrorMessage(err.Error())
	}
	
	removedCount := 0
	for i := 2; i < len(command); i++ {
		member := command[i]
		if sortedSet.Remove(member) {
			removedCount++
		}
	}
	
	return resp.EncodeResp(int64(removedCount))

}




