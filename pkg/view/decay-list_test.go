package view

import "testing"

func TestDecayListAdd(t *testing.T) {
	list := DecayList{}

	list.Add(1, DecayEvent{TimeStamp: 100, Value: 50.0})
	list.Add(1, DecayEvent{TimeStamp: 101, Value: 60.0})
	list.Add(2, DecayEvent{TimeStamp: 102, Value: 70.0})
	list.Add(2, DecayEvent{TimeStamp: 103, Value: 80.0})

	if len(list[1]) != 2 {
		t.Errorf("Expected 2 events for key 1, got %d", len(list[1]))
	}
	if len(list[2]) != 2 {
		t.Errorf("Expected 2 events for key 2, got %d", len(list[2]))
	}
	value := list.Decay(4000)
	if len(value) != 2 {
		t.Errorf("Expected 2 events for key 1, got %d", len(value))
	}

}
