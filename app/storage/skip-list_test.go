package storage

import (
	"testing"
)

func TestSkipList_Insert(t *testing.T) {
	skipList := NewSkipList[string]()
	skipList.insert(1.0, "1")
	skipList.insert(2.0, "2")
	skipList.insert(3.0, "3")
	skipList.insert(2.5, "2.5")

	t.Log(skipList.String())

	if skipList.length != 4 {
		t.Errorf("expected length 4, got %d", skipList.length)
	}
	if skipList.floors[0].head.score != 1.0 {
		t.Errorf("expected head score 1.0, got %f", skipList.floors[0].head.score)
	}
	if skipList.floors[0].head.value != "1" {
		t.Errorf("expected head value 1, got %s", skipList.floors[0].head.value)
	}
	if skipList.floors[0].head.next.score != 2.0 {
		t.Errorf("expected second node score 2.0, got %f", skipList.floors[0].head.next.score)
	}
	if skipList.floors[0].head.next.value != "2" {
		t.Errorf("expected second node value 2, got %s", skipList.floors[0].head.next.value)
	}
	if skipList.floors[0].tail.score != 3.0 {
		t.Errorf("expected tail score 3.0, got %f", skipList.floors[0].tail.score)
	}
	if skipList.floors[0].tail.value != "3" {
		t.Errorf("expected tail value 3, got %s", skipList.floors[0].tail.value)
	}

	if skipList.floors[0].head.next.next.score != 2.5 {
		t.Errorf("expected third node score 2.5, got %f", skipList.floors[0].head.next.next.score)
	}
}

func TestSkipList_Rank(t *testing.T) {
	skipList := NewSkipList[string]()
	skipList.Add(1.0, "1")
	skipList.Add(2.0, "2")
	skipList.Add(3.0, "3")
	
	rank := skipList.Rank("2")
	if rank != 1 {
		t.Errorf("expected rank 1 for value 2, got %d", rank)
	}

	rank = skipList.Rank("4")
	if rank != -1 {
		t.Errorf("expected rank -1 for non-existent value 4, got %d", rank)
	}
}

func TestSkipList_remove(t *testing.T) {
	skipList := NewSkipList[string]()
	skipList.insert(1.0, "1")
	skipList.insert(2.0, "2")
	skipList.insert(3.0, "3")
	
	removed := skipList.delete("2")
	if !removed {
		t.Errorf("expected to remove value 2, but it was not removed")
	}
	if skipList.length != 2 {
		t.Errorf("expected length 2 after removal, got %d", skipList.length)
	}
	if skipList.floors[0].head.score != 1.0 {
		t.Errorf("expected head score 1.0 after removal, got %f", skipList.floors[0].head.score)
	}
	if skipList.floors[0].head.value != "1" {
		t.Errorf("expected head value 1 after removal, got %s", skipList.floors[0].head.value)
	}
	if skipList.floors[0].tail.score != 3.0 {
		t.Errorf("expected tail score 3.0 after removal, got %f", skipList.floors[0].tail.score)
	}
	if skipList.floors[0].tail.value != "3" {
		t.Errorf("expected tail value 3 after removal, got %s", skipList.floors[0].tail.value)
	}
	
	removed = skipList.delete("4")
	if removed {
		t.Errorf("expected to not remove value 4, but it was removed")
	}
	if skipList.length != 2 {
		t.Errorf("expected length to remain 2 after failed removal, got %d", skipList.length)
	}
}
