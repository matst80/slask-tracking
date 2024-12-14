package view

import (
	"math"
	"testing"
)

func TestDecayEvent_CalculateValue(t *testing.T) {
	tests := []struct {
		name       string
		event      DecayEvent
		now        int64
		wantResult float64
	}{
		{
			name:       "no decay",
			event:      DecayEvent{TimeStamp: 100, Value: 50.0},
			now:        100,
			wantResult: 50.0,
		},
		{
			name:       "positive decay",
			event:      DecayEvent{TimeStamp: 100, Value: 50.0},
			now:        101,
			wantResult: 50.0 * math.Pow(decayRate, 1),
		},
		{
			name:       "multiple decay steps",
			event:      DecayEvent{TimeStamp: 100, Value: 50.0},
			now:        105,
			wantResult: 50.0 * math.Pow(decayRate, 5),
		},
		{
			name:       "no decay with negative time delta",
			event:      DecayEvent{TimeStamp: 150, Value: 50.0},
			now:        100,
			wantResult: 50.0,
		},
		{
			name:       "zero initial value",
			event:      DecayEvent{TimeStamp: 100, Value: 0.0},
			now:        105,
			wantResult: 0.0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := tt.event.CalculateValue(tt.now)
			if math.Abs(got-tt.wantResult) > 1e-9 {
				t.Errorf("CalculateValue() = %v, want %v", got, tt.wantResult)
			}
		})
	}
	t.Run("Decay after one day", func(t *testing.T) {
		event := DecayEvent{TimeStamp: 100, Value: 100.0}
		event.Decay(100 + 60*60*24)
		if event.Value < 50.0 {
			t.Errorf("Decayed value %v, want over 50", event.Value)
		}
		if event.Value > 70.0 {
			t.Errorf("Decayed value %v, want below 70", event.Value)
		}
	})
	t.Run("Decay after one week", func(t *testing.T) {
		event := DecayEvent{TimeStamp: 100, Value: 100.0}
		event.Decay(100 + 60*60*24*7)
		if event.Value < 50.0 {
			t.Errorf("Decayed value %v, want over 50", event.Value)
		}
		if event.Value > 70.0 {
			t.Errorf("Decayed value %v, want below 70", event.Value)
		}
	})
}
