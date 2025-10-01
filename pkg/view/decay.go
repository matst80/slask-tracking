package view

import (
	"math"

	"github.com/matst80/slask-finder/pkg/sorting"
)

type DecayEvent struct {
	TimeStamp int64   `json:"ts"`
	Value     float64 `json:"value"`
}

const (
	decayRate = 0.9999995
	maxAge    = 60 * 60 * 24 * 48
)

func (d *DecayEvent) CalculateValue(now int64) float64 {

	// Calculate the time difference between the event timestamp and the current time.
	timeElapsed := now - d.TimeStamp
	if timeElapsed < 0 {
		// If the timestamp is in the future, return the original value.
		return d.Value
	}
	if timeElapsed > maxAge {
		return 0
	}

	// Apply exponential decay formula.
	decayedValue := d.Value * math.Pow(decayRate, float64(timeElapsed))

	return decayedValue
}

func (d *DecayEvent) Decay(now int64) float64 {
	v := d.CalculateValue(now)

	//if v < 0.1 {
	//	d.TimeStamp = now
	//	d.Value = v
	//}
	return v
}

type DecayArray = []DecayEvent

type DecayPopularity struct {
	Events DecayArray `json:"events"`
	Value  float64    `json:"value"`
}

func (d *DecayPopularity) Add(value DecayEvent) {
	if d.Events == nil {
		d.Events = make([]DecayEvent, 0)
	}
	d.Events = append(d.Events, value)
}

func (d *DecayPopularity) Decay(now int64) float64 {

	var popularity float64

	for _, event := range d.Events {
		popularity += event.Decay(now)
	}
	d.Value = popularity
	return popularity
}

func (d *DecayPopularity) RemoveOlderThan(when int64) {
	end := len(d.Events)

	for i, e := range d.Events {
		if e.TimeStamp >= when {
			end = i
		}
	}
	d.Events = d.Events[:end]
}

type DecayList map[uint]DecayArray

func (d *DecayList) Add(key uint, value DecayEvent) {
	f, ok := (*d)[key]
	if !ok {
		(*d)[key] = []DecayEvent{
			value,
		}
	} else {
		//f = append(f, value)
		(*d)[key] = append(f, value)
	}
}

func (d *DecayList) Decay(now int64) sorting.SortOverride {
	result := sorting.SortOverride{}
	var popularity float64
	var event DecayEvent

	for itemId, events := range *d {
		popularity = 0
		for _, event = range events {
			popularity += event.Decay(now)
		}
		if popularity < 0.002 {
			continue
		}
		result[itemId] = popularity

	}
	// *d = slices.DeleteFunc(d, func(i DecayEvent) bool {
	// 	// log.Printf("Deleting value popularity %s for query %s, value:%f", key, q, value.Value)
	// 	return i.Value < 0.0002
	// })

	return result
}
