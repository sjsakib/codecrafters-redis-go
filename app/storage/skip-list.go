package storage

import "strings"

import "fmt"

type Node[T comparable] struct {
	score float64
	value T
	next  *Node[T]
	prev  *Node[T]
	top   *Node[T]
	down  *Node[T]
}

func (n *Node[T]) Compare(other *Node[T]) int {
	if n.score < other.score {
		return -1
	}
	if n.score > other.score {
		return 1
	}
	// if T is string do lexicographical comparison
	if str1, ok1 := any(n.value).(string); ok1 {
		if str2, ok2 := any(other.value).(string); ok2 {
			return strings.Compare(str1, str2)
		}
	}
	return 0
}

type Floor[T comparable] struct {
	head *Node[T]
	tail *Node[T]
}

type SkipList[T comparable] struct {
	floors []*Floor[T]

	scoreMap map[T]float64

	length int
	height int
}

// just a doubly linked list for now
func NewSkipList[T comparable]() *SkipList[T] {
	floor := &Floor[T]{
		head: nil,
		tail: nil,
	}

	return &SkipList[T]{
		floors:   []*Floor[T]{floor},
		length:   0,
		height:   1,
		scoreMap: make(map[T]float64),
	}
}

func (s *SkipList[T]) Add(score float64, value T) bool {
	existingScore, exists := s.scoreMap[value]
	if exists && score == existingScore {
		return false
	}
	if exists {
		s.delete(value)
	}
	s.insert(score, value)
	s.scoreMap[value] = score
	return !exists
}

func (s *SkipList[T]) Remove(value T) bool {
	removed := s.delete(value)
	if removed {
		delete(s.scoreMap, value)
	}
	return removed
}

func (s *SkipList[T]) Rank(value T) int {
	_, exists := s.scoreMap[value]
	if !exists {
		return -1
	}

	ground := s.floors[0]
	rank := 0
	cur := ground.head
	for cur != nil && cur.value != value {
		rank++
		cur = cur.next
	}
	return rank

}

func (s *SkipList[T]) Range(start, stop int) []T {
	ground := s.floors[0]
	cur := ground.head
	result := make([]T, 0)
	index := 0
	if start < 0 {
		start = s.length + start
	}
	if stop < 0 {
		stop = s.length + stop
	}
	for cur != nil {
		if index >= start && index <= stop {
			result = append(result, cur.value)
		}
		if index > stop {
			break
		}
		index++
		cur = cur.next
	}
	return result
}

func (s *SkipList[T]) GetScore(value T) (float64, bool) {
	score, exists := s.scoreMap[value]
	return score, exists
}

func (s *SkipList[T]) insert(score float64, value T) {
	ground := s.floors[0]
	node := &Node[T]{
		score: score,
		value: value,
	}

	s.length++

	if ground.head == nil {
		ground.head = node
		ground.tail = node
		return
	}

	cur := ground.head

	if cur.Compare(node) > 0 {
		node.next = cur
		node.prev = cur.prev
		cur.prev = node
		ground.head = node

		return
	}

	for cur.next != nil && cur.next.Compare(node) < 0 {
		cur = cur.next
	}

	node.next = cur.next
	if cur.next != nil {
		cur.next.prev = node
	}
	cur.next = node
	node.prev = cur

	if node.next == nil {
		ground.tail = node
	}
}

func (s *SkipList[T]) delete(value T) bool {
	ground := s.floors[0]
	cur := ground.head
	for cur != nil {
		if cur.value == value {
			if cur.prev != nil {
				cur.prev.next = cur.next
			} else {
				ground.head = cur.next
			}
			if cur.next != nil {
				cur.next.prev = cur.prev
			} else {
				ground.tail = cur.prev
			}
			s.length--
			return true
		}
		cur = cur.next
	}
	return false
}

func (s *SkipList[T]) Size() int {
	return s.length
}

func (s *SkipList[T]) String() string {
	var result strings.Builder
	for i, floor := range s.floors {
		fmt.Fprintf(&result, "Floor %d: ", i)
		cur := floor.head
		for cur != nil {
			fmt.Fprintf(&result, "(%f, %v) ", cur.score, cur.value)
			cur = cur.next
		}
		result.WriteString("\n")
	}
	return result.String()
}
